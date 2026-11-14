"""FastAPI 入口：批管理 REST API + Swagger 文档 + DICOM 服务生命周期。"""
from __future__ import annotations

import logging
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, HTTPException, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from . import deid, dicom, storage
from .config import settings
from .db import (
    DEID_COMPLETED,
    DEID_FAILED,
    DEID_RUNNING,
    STATE_COMPLETED,
    Database,
    set_db,
)
from .pipeline import Pipeline, recover_on_startup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
)
log = logging.getLogger("api")

# 进程级单例（lifespan 中初始化）
db: Database | None = None
pipeline: Pipeline | None = None
_scp_server = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db, pipeline, _scp_server
    settings.ensure_dirs()
    db = Database(settings.DB_PATH)
    set_db(db)
    recover_on_startup(db)
    deid.recover_on_startup(db)
    _scp_server = dicom.start_store_scp(db)
    pipeline = Pipeline(db)
    log.info("API 就绪，数据卷=%s", settings.DATA_DIR)
    try:
        yield
    finally:
        if _scp_server is not None:
            _scp_server.shutdown()
        if db is not None:
            db.close()


app = FastAPI(
    title="DICOM CT 离线取数流水线",
    version="1.0.0",
    lifespan=lifespan,
    description=(
        "提交 StudyInstanceUID → C-FIND 固定 SOP 清单 → C-MOVE 回传 → "
        "C-STORE 接收非压缩 CT → 核对收齐后打包 ZIP 下载。\n\n"
        "规则：清单外实例不入包、重复实例不重复计数、保存失败即批失败、"
        "失败批迟到回传不可恢复、重启后未完成批判失败。\n\n"
        "定向去标识（de-identification）：对已完成批的**原 ZIP**按批生成"
        "可交付院外分割组的新 ZIP——递归删除私有标签及 PatientName/"
        "PatientBirthDate/AccessionNumber，重新分配 PatientID 与 "
        "Study/Series/SOP UID（同批重试一致），同步改写批内引用、文件"
        "元信息、文件名与清单；引用指向批外实例时拒绝发布并说明原因；"
        "像素与几何标签保持原值；原包仍可下载。"
    ),
)


# ------------------------------------------------------------------ schemas
class BatchCreate(BaseModel):
    study_instance_uid: str = Field(
        ..., description="源端检查的 StudyInstanceUID",
        examples=["1.2.840.10008.1.2.99.1"],
    )
    fault_inject: str = Field(
        default="",
        description="可选故障注入：save_failure=保存失败演示；留空为正常流程",
    )


class BatchView(BaseModel):
    id: str
    study_instance_uid: str
    state: str
    expected_count: int
    received_count: int
    failure_reason: str | None
    zip_path: str | None
    fault_inject: str
    created_at: str
    updated_at: str
    downloadable: bool


class DeidView(BaseModel):
    batch_id: str
    state: str | None = Field(
        None, description="去标识包状态：None(未生成)/RUNNING/COMPLETED/FAILED")
    downloadable: bool
    error: str | None = Field(None, description="失败或拒绝发布的原因")
    reason_code: str | None = Field(
        None,
        description="失败类别：EXTERNAL_REFERENCE=引用指向批外实例（拒绝发布）；"
                    "GENERATION_FAILED=读写/解析失败（可重试）",
    )
    retryable: bool = Field(
        False, description="失败后是否可再次调用生成（True=可重试）")
    attempt: int = Field(0, description="第几次生成尝试（失败可重试）")
    new_study_instance_uid: str | None = None
    new_patient_id: str | None = None
    instance_count: int = 0


class DeidInstanceMap(BaseModel):
    original_sop_instance_uid: str
    new_sop_instance_uid: str
    file_name: str


def _view(row: dict) -> BatchView:
    return BatchView(
        id=row["id"],
        study_instance_uid=row["study_instance_uid"],
        state=row["state"],
        expected_count=row["expected_count"],
        received_count=row["received_count"],
        failure_reason=row["failure_reason"],
        zip_path=row["zip_path"],
        fault_inject=row["fault_inject"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        downloadable=row["state"] == STATE_COMPLETED and bool(row["zip_path"]),
    )


def _deid_reason_code(error: str | None, state: str | None) -> str | None:
    if state != DEID_FAILED:
        return None
    return "EXTERNAL_REFERENCE" if error and "本批之外" in error \
        else "GENERATION_FAILED"


def _deid_view(batch_id: str) -> DeidView:
    row = db.get_deid(batch_id)
    if row is None:
        return DeidView(batch_id=batch_id, state=None, downloadable=False,
                        instance_count=0)
    return DeidView(
        batch_id=batch_id,
        state=row["state"],
        downloadable=(row["state"] == DEID_COMPLETED and bool(row["zip_path"])
                      and Path(row["zip_path"]).exists()),
        error=row["error"],
        reason_code=_deid_reason_code(row["error"], row["state"]),
        retryable=row["state"] == DEID_FAILED,
        attempt=row["attempt"],
        new_study_instance_uid=row["study_uid"],
        new_patient_id=row["patient_id"],
        instance_count=len(db.deid_instances(batch_id)),
    )


def _require_completed(batch_id: str) -> dict:
    row = db.get_batch(batch_id)
    if row is None:
        raise HTTPException(404, "批不存在")
    if row["state"] != STATE_COMPLETED or not row["zip_path"]:
        raise HTTPException(
            409,
            f"批状态 {row['state']} 不可发布去标识包；"
            "必须原包已收齐并成功生成（COMPLETED）",
        )
    return row


# ------------------------------------------------------------------ routes
@app.post("/batches", response_model=BatchView, status_code=201,
          tags=["batches"], summary="提交检查，启动取数批")
def create_batch(body: BatchCreate) -> BatchView:
    if body.fault_inject not in ("", storage.FAULT_SAVE):
        raise HTTPException(400, "fault_inject 只支持 '' 或 'save_failure'")
    uid = body.study_instance_uid.strip()
    if not uid:
        raise HTTPException(400, "study_instance_uid 不能为空")

    if db.has_active_batch_for_study(uid):
        raise HTTPException(409, "该 Study 已有进行中的批，请等待其结束")

    batch_id = uuid.uuid4().hex
    db.create_batch(batch_id, uid, body.fault_inject)
    pipeline.submit(batch_id)
    return _view(db.get_batch(batch_id))


@app.get("/batches", response_model=list[BatchView],
         tags=["batches"], summary="列出全部批")
def list_batches() -> list[BatchView]:
    return [_view(r) for r in db.list_batches()]


@app.get("/batches/{batch_id}", response_model=BatchView,
         tags=["batches"], summary="查询批状态与进度")
def get_batch(batch_id: str) -> BatchView:
    row = db.get_batch(batch_id)
    if row is None:
        raise HTTPException(404, "批不存在")
    return _view(row)


@app.get("/batches/{batch_id}/events",
         tags=["batches"], summary="查询批事件流水（接收/重复/拒收/状态迁移）")
def get_events(batch_id: str) -> list[dict]:
    if db.get_batch(batch_id) is None:
        raise HTTPException(404, "批不存在")
    return db.list_events(batch_id)


@app.get("/batches/{batch_id}/instances",
         tags=["batches"], summary="查询批清单及每个 SOP 的接收情况")
def get_instances(batch_id: str) -> dict:
    row = db.get_batch(batch_id)
    if row is None:
        raise HTTPException(404, "批不存在")
    files = db.received_files(batch_id)
    received = {sop: path for sop, path in files}
    return {
        "batch_id": batch_id,
        "state": row["state"],
        "manifest": sorted(db.manifest_uids(batch_id)),
        "received": sorted(received),
        "missing": db.missing_uids(batch_id),
    }


@app.get("/batches/{batch_id}/download",
         tags=["batches"], summary="下载 ZIP（仅 COMPLETED 批可下载）")
def download_zip(batch_id: str) -> FileResponse:
    row = db.get_batch(batch_id)
    if row is None:
        raise HTTPException(404, "批不存在")
    if row["state"] != STATE_COMPLETED or not row["zip_path"]:
        raise HTTPException(
            409,
            f"批状态 {row['state']} 不可下载；仅 C-MOVE 成功且清单收齐的批可下载",
        )
    path = Path(row["zip_path"])
    if not path.exists():
        raise HTTPException(410, "ZIP 文件缺失（数据卷可能已损坏）")
    return FileResponse(
        path, media_type="application/zip",
        filename=f"{batch_id}.zip",
    )


# ------------------------------------------------------------ deid packages
@app.post(
    "/batches/{batch_id}/deid-package",
    response_model=DeidView,
    tags=["de-identification"],
    summary="按批生成定向去标识包（幂等；失败可重试）",
    responses={
        201: {"model": DeidView, "description": "首次生成并原子发布完成"},
        200: {"model": DeidView, "description": "已存在同一份完整新包（幂等返回）"},
        409: {"description": "原批未完成，或另一请求正在生成"},
        422: {"description": "引用指向批外实例，拒绝发布（含原因）"},
    },
)
def create_deid_package(batch_id: str, response: Response) -> DeidView:
    """读取原 ZIP，生成可交付给院外分割组的去标识新包。

    * 递归删除私有标签及 PatientName / PatientBirthDate / AccessionNumber；
    * PatientID 与 Study/Series/SOP UID 换新，同批重试结果一致；
    * 序列内指向本批实例的引用、文件元信息、文件名与清单同步改写；
    * 引用指向批外实例时拒绝发布并说明原因（HTTP 422）；
    * 像素与几何标签保持原值。

    并发：同一批同时只有一份新包会被发布；其他请求等待并复用同一结果。
    已完成的包重复调用直接返回（幂等）；失败后可再次调用重试。
    """
    _require_completed(batch_id)

    existing = db.get_deid(batch_id)
    result = deid.generate(db, batch_id)
    if result.state == DEID_RUNNING:
        raise HTTPException(409, "去标识包正在生成中，请稍后重试")
    if result.state == DEID_FAILED:
        raise HTTPException(
            422,
            detail={
                "message": result.error or "去标识包生成失败",
                "state": result.state,
                "reason_code": ("EXTERNAL_REFERENCE" if result.rejected
                                else "GENERATION_FAILED"),
                "attempt": result.attempt,
                "retryable": True,
            },
        )
    # 首次发布（或失败后重试成功）返回 201；复用已发布包返回 200
    if result.created and not (
        existing is not None and existing["state"] == DEID_COMPLETED
    ):
        response.status_code = 201
    return _deid_view(batch_id)


@app.get(
    "/batches/{batch_id}/deid-package",
    response_model=DeidView,
    tags=["de-identification"],
    summary="查询去标识包状态（未生成/生成中/完成/失败及原因）",
)
def get_deid_package(batch_id: str) -> DeidView:
    if db.get_batch(batch_id) is None:
        raise HTTPException(404, "批不存在")
    return _deid_view(batch_id)


@app.get(
    "/batches/{batch_id}/deid-package/instances",
    response_model=list[DeidInstanceMap],
    tags=["de-identification"],
    summary="查询旧 SOP -> 新 SOP 的映射（文件名与清单来源）",
)
def get_deid_instances(batch_id: str) -> list[dict]:
    if db.get_batch(batch_id) is None:
        raise HTTPException(404, "批不存在")
    return db.deid_instances(batch_id)


@app.get(
    "/batches/{batch_id}/deid-download",
    tags=["de-identification"],
    summary="下载已发布的去标识 ZIP（原包下载不受影响）",
)
def download_deid(batch_id: str) -> FileResponse:
    if db.get_batch(batch_id) is None:
        raise HTTPException(404, "批不存在")
    row = db.get_deid(batch_id)
    if row is None or row["state"] == DEID_FAILED:
        raise HTTPException(
            409,
            detail={
                "message": row["error"] if row else "尚未生成去标识包",
                "state": row["state"] if row else None,
                "retryable": True,
            },
        )
    if row["state"] == DEID_RUNNING or not row["zip_path"]:
        raise HTTPException(409, "去标识包正在生成中，尚未发布")
    path = Path(row["zip_path"])
    if not path.exists():
        raise HTTPException(410, "已发布的去标识包缺失（数据卷可能已损坏）")
    return FileResponse(
        path, media_type="application/zip",
        filename=f"{batch_id}.deid.zip",
    )


@app.get("/health", tags=["system"], summary="健康检查")
def health() -> dict:
    return {"status": "ok", "store_port": settings.STORE_PORT,
            "ae_title": settings.APP_AET}


# ------------------------------------------------------------------ debug
@app.get("/internal/pacs-studies", tags=["internal"],
         summary="（演示用）STUDY 级 C-FIND 列出模拟 PACS 中的检查")
def pacs_studies() -> list[dict]:
    from pydicom.dataset import Dataset
    from pydicom.uid import ExplicitVRLittleEndian, ImplicitVRLittleEndian
    from pynetdicom import AE
    from pynetdicom.presentation import build_context
    from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelFind

    ae = AE(ae_title=settings.APP_AET)
    ae.requested_contexts = [build_context(
        str(StudyRootQueryRetrieveInformationModelFind),
        [str(ExplicitVRLittleEndian), str(ImplicitVRLittleEndian)])]
    assoc = ae.associate(settings.PACS_HOST, settings.PACS_PORT,
                         ae_title=settings.PACS_AET)
    if not assoc.is_established:
        raise HTTPException(502, "无法连接模拟 PACS")
    try:
        q = Dataset()
        q.QueryRetrieveLevel = "STUDY"
        q.StudyInstanceUID = ""
        q.PatientID = ""
        q.PatientName = ""
        q.Modality = ""
        q.NumberOfStudyRelatedInstances = ""
        out = []
        for status, ident in assoc.send_c_find(
            q, str(StudyRootQueryRetrieveInformationModelFind)
        ):
            if ident is None:
                continue
            out.append({
                "study_instance_uid": str(ident.StudyInstanceUID),
                "patient_id": str(getattr(ident, "PatientID", "")),
                "patient_name": str(getattr(ident, "PatientName", "")),
                "modality": str(getattr(ident, "Modality", "")),
                "instances": int(getattr(ident, "NumberOfStudyRelatedInstances", 0) or 0),
            })
        return out
    finally:
        if assoc.is_alive:
            assoc.release()


@app.post("/internal/suicide", tags=["internal"],
          summary="（演示用）退出进程以演示重启恢复；compose 自动重启")
def suicide(tasks: BackgroundTasks) -> dict:
    def _die() -> None:
        os._exit(1)
    tasks.add_task(_die)
    return {"detail": "进程即将退出，等待 Docker 重启"}
