"""按批生成并原子发布定向去标识包。

本模块负责把“原 ZIP”转成“可交付给院外分割组的新 ZIP”。关键不变量：

* **原子发布**：新包先写到同目录下的临时文件（``.part``），完整写入并
  fsync 后，才用 ``os.replace`` 改名到最终路径。任何写盘失败都只留下临时
  文件（随后清理），最终路径要么不存在、要么是完整包。
* **并发单发布者**：数据库 ``claim_deid`` 原子领取生成权，同一批只有一个
  线程/请求能进入发布流程；其他请求要么等到同一份完整包，要么得到明确的
  “生成中”状态。
* **重启安全**：启动时把残留的 RUNNING 置为 FAILED，并清理临时文件；
  已发布的 COMPLETED 包仍在，可继续下载；FAILED 允许重试。
"""
from __future__ import annotations

import logging
import os
import threading
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path

import io

from . import anonymize
from .config import settings
from .db import Database, DEID_COMPLETED, DEID_FAILED, DEID_RUNNING

log = logging.getLogger("deid")


class DeidError(Exception):
    """去标识生成失败（可重试）。"""


@dataclass
class DeidResult:
    state: str
    zip_path: str | None
    error: str | None
    study_uid: str | None = None
    patient_id: str | None = None
    attempt: int = 0
    created: bool = False   # 本次调用是否真正完成了发布（False=复用已有包）
    rejected: bool = False  # 是否因批外引用等安全规则被拒绝（区别于写盘失败）


def deid_zip_path(batch_id: str) -> Path:
    return settings.ZIP_DIR / f"{batch_id}.deid.zip"


def _deid_tmp_path(batch_id: str) -> Path:
    return settings.ZIP_DIR / f"{batch_id}.deid.zip.part"


def _fsync_dir(d: Path) -> None:
    try:
        fd = os.open(str(d), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def _fsync_file(path: Path) -> None:
    with open(path, "rb") as fh:
        os.fsync(fh.fileno())


def _zip_info(name: str) -> zipfile.ZipInfo:
    """固定时间戳，保证同批重试产出的包在内容层面完全一致。"""
    info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o600 << 16
    return info


def _write_zip(tmp_path: Path, plan: anonymize.Plan) -> None:
    """把去标识后的实例与清单写入临时 ZIP（不发布）。

    交付包内只出现新 UID：旧 SOP -> 新 SOP 的映射仅保留在服务端数据库
    （``GET /batches/{id}/deid-package/instances``），避免交付物本身仍可
    回溯到原检查。
    """
    manifest = ["sop_instance_uid,file_name"]
    used: set[str] = set()
    with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for item in plan.instances:
            name = item.file_name
            if name in used:  # 理论兜底：文件名冲突时用新 SOP 唯一化
                name = f"{item.new_sop_uid.replace('.', '_')}.dcm"
            used.add(name)
            zf.writestr(_zip_info(f"dicom/{name}"), _serialize(item))
            manifest.append(f"{item.new_sop_uid},{name}")
        zf.writestr(_zip_info("MANIFEST.csv"), "\n".join(manifest) + "\n")
        zf.writestr(
            _zip_info("DEID_INFO.txt"),
            "\n".join([
                f"new_study_instance_uid={plan.study_uid}",
                f"new_patient_id={plan.patient_id}",
                "removed=all private tags (recursively); PatientName; "
                "PatientBirthDate; AccessionNumber",
                "reassigned=PatientID; Study/Series/SOPInstanceUID; "
                "in-batch instance references; file meta; file names",
                "preserved=pixel data and all geometry tags",
            ]) + "\n",
        )


def _serialize(item: anonymize.PreparedInstance) -> bytes:
    """按原传输语法序列化（保持未压缩 little-endian）。"""
    buffer = io.BytesIO()
    item.dataset.save_as(buffer, enforce_file_format=True)
    return buffer.getvalue()


def generate(
    db: Database, batch_id: str, wait_timeout: float = 30.0
) -> DeidResult:
    """生成（或读取已发布的）去标识包。

    并发语义：只有一个调用能原子领取 RUNNING 并真正写盘；其余调用会
    等待该发布者结束——成功则返回同一份完整包，失败则尝试重新领取。
    若等待超时，返回 RUNNING 状态（调用方可稍后查询/下载）。
    """
    current = db.get_deid(batch_id)
    if current and current["state"] == DEID_COMPLETED:
        return _result(current)

    if db.claim_deid(batch_id):
        return _publish(db, batch_id)

    # 没抢到生成权：另一请求正在发布（或状态刚变化）。等待其落定。
    deadline = time.monotonic() + wait_timeout
    while time.monotonic() < deadline:
        row = db.get_deid(batch_id)
        if row is None:
            raise DeidError("领取去标识生成权失败")
        if row["state"] == DEID_COMPLETED:
            return _result(row)
        if row["state"] == DEID_FAILED:
            if db.claim_deid(batch_id):  # 上一轮已失败：本次重试
                return _publish(db, batch_id)
        time.sleep(0.05)
    return _result(db.get_deid(batch_id), state_override=DEID_RUNNING)


def _publish(db: Database, batch_id: str) -> DeidResult:
    tmp_path = _deid_tmp_path(batch_id)
    final_path = deid_zip_path(batch_id)
    batch = db.get_batch(batch_id)
    if batch is None or not batch.get("zip_path"):
        reason = "原包不存在：批未完成或 ZIP 未生成，拒绝发布去标识包"
        db.fail_deid(batch_id, reason)
        _cleanup(tmp_path)
        return _result(db.get_deid(batch_id))
    if not Path(batch["zip_path"]).exists():
        reason = "原包文件缺失（数据卷可能已损坏），拒绝发布去标识包"
        db.fail_deid(batch_id, reason)
        _cleanup(tmp_path)
        return _result(db.get_deid(batch_id))

    try:
        datasets = anonymize.read_dicom_from_zip(batch["zip_path"])
        plan = anonymize.prepare_datasets(
            batch_id, datasets,
            uid_root=settings.ANON_UID_ROOT,
            patient_prefix=settings.ANON_PATIENT_PREFIX,
        )
        _write_zip(tmp_path, plan)
        _fsync_file(tmp_path)
        # 原子发布：改名之前任何失败都不会产生“半成品最终包”
        os.replace(tmp_path, final_path)
        _fsync_dir(final_path.parent)

        ok = db.complete_deid(
            batch_id, str(final_path),
            study_uid=plan.study_uid,
            patient_id=plan.patient_id,
            instances=[
                (i.original_sop_uid, i.new_sop_uid, i.file_name)
                for i in plan.instances
            ],
        )
        if not ok:
            # 状态已被其他流程改写（理论竞态）：撤销本次发布，避免暴露孤儿文件
            _remove_quietly(final_path)
            raise DeidError("去标识结果登记失败，已撤销发布")
        db.add_event(
            "DEID_PUBLISHED",
            f"定向去标识包已发布：{len(plan.instances)} 个实例，"
            f"new Study={plan.study_uid}，new PatientID={plan.patient_id}",
            batch_id,
        )
        log.info("批 %s 去标识包发布：%s", batch_id, final_path)
        return _result(db.get_deid(batch_id), created=True)

    except anonymize.DedupError as exc:
        reason = str(exc)
        _cleanup(tmp_path)
        db.fail_deid(batch_id, reason)
        db.add_event("DEID_REJECTED", reason, batch_id)
        log.warning("批 %s 拒绝发布去标识包：%s", batch_id, reason)
        return _result(db.get_deid(batch_id), rejected=True)
    except (OSError, DeidError) as exc:
        reason = f"去标识包写盘/发布失败：{exc}"
        _cleanup(tmp_path)
        db.fail_deid(batch_id, reason)
        db.add_event("DEID_FAILED", reason, batch_id)
        log.exception("批 %s 去标识包生成失败", batch_id)
        return _result(db.get_deid(batch_id))
    except Exception as exc:  # 解析异常等，同样可重试
        reason = f"去标识包生成异常：{exc}"
        _cleanup(tmp_path)
        db.fail_deid(batch_id, reason)
        db.add_event("DEID_FAILED", reason, batch_id)
        log.exception("批 %s 去标识包生成异常", batch_id)
        return _result(db.get_deid(batch_id))


def _cleanup(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _remove_quietly(path: Path) -> None:
    _cleanup(path)


def _result(
    row: dict | None,
    *,
    state_override: str | None = None,
    created: bool = False,
    rejected: bool = False,
) -> DeidResult:
    if row is None:
        return DeidResult(state=DEID_FAILED, zip_path=None,
                          error="去标识状态缺失")
    return DeidResult(
        state=state_override or row["state"],
        zip_path=row.get("zip_path"),
        error=row.get("error"),
        study_uid=row.get("study_uid"),
        patient_id=row.get("patient_id"),
        attempt=int(row.get("attempt") or 0),
        created=created,
        rejected=rejected,
    )


# ------------------------------------------------------------------ recovery
_recovery_lock = threading.Lock()


def recover_on_startup(db: Database) -> list[str]:
    """重启恢复。

    * RUNNING（进程在发布前被杀）一律置 FAILED，可重试；
    * 清理所有残留临时包（最终路径永不可能含半成品）；
    * COMPLETED 且有最终包的保留；最终包丢失则置 FAILED 以便重新生成；
    * **孤儿包**：最终包存在但数据库无对应的 COMPLETED 记录（例如
      ``os.replace`` 成功后、登记前进程被强杀），删除该文件——它从未被
      发布，不能被当作可下载包暴露出去。
    """
    recovered: list[str] = []
    with _recovery_lock:
        leftover = 0
        for part in settings.ZIP_DIR.glob("*.deid.zip.part"):
            try:
                part.unlink()
                leftover += 1
            except OSError:
                pass
        if leftover:
            log.info("启动清理 %d 个未发布的去标识临时包", leftover)

        recovered = db.recover_deid_running(
            "服务重启导致去标识包发布中断；未暴露半成品，可重试"
        )
        for batch_id in recovered:
            db.add_event(
                "DEID_FAILED",
                "服务重启导致去标识包发布中断；未暴露半成品，可重试",
                batch_id,
            )

        for row in db.all_deid_packages():
            if row["state"] == DEID_COMPLETED:
                path = row.get("zip_path")
                if not path or not Path(path).exists():
                    if db.invalidate_deid_package(
                        row["batch_id"], "已发布包文件缺失，可重新生成"
                    ):
                        recovered.append(row["batch_id"])

        # 孤儿最终包：数据库中没有处于 COMPLETED 的对应记录
        published = {
            row["batch_id"] for row in db.all_deid_packages()
            if row["state"] == DEID_COMPLETED
        }
        for final in settings.ZIP_DIR.glob("*.deid.zip"):
            batch_id = final.name.removesuffix(".deid.zip")
            if batch_id not in published:
                try:
                    final.unlink()
                    log.warning("启动清理未登记的孤儿去标识包：%s", final.name)
                    db.add_event(
                        "DEID_ORPHAN_REMOVED",
                        "启动清理未登记的孤儿去标识包（从未发布）",
                        batch_id,
                    )
                except OSError:
                    log.exception("清理孤儿去标识包失败：%s", final)
    return recovered
