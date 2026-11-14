"""定向去标识（de-identification）核心逻辑。

输入是一批原始 CT 数据集，输出是同一批实例的“可发布副本”：

* 递归删除全部私有标签（含嵌套序列里的一切私有标签）；
* 递归删除直接标识 ``PatientName``、``PatientBirthDate``、``AccessionNumber``；
* 为 ``PatientID`` 与 Study/Series/SOP UID 分配新值。新值只由
  ``(批 ID, 旧值)`` 决定，因此**同批重试得到完全相同的新值**，
  不同批之间则彼此独立；
* 同步改写序列内指向本批实例的引用（Referenced SOP / Series / Study），
  改写文件元信息与文件名；
* 引用指向**批外**实例时拒绝发布并说明原因。

像素数据与几何标签（Rows/Columns/PixelSpacing/ImagePositionPatient/
SliceLocation 等）保持原值——本模块不触碰 (7FE0,0010) 及任何几何标签。
"""
from __future__ import annotations

import hashlib
import io
import re
from dataclasses import dataclass, field

import pydicom
from pydicom.dataset import Dataset
from pydicom.sequence import Sequence

# ------------------------------------------------------------------ 删除清单
# 直接标识：递归删除，即使为空也删除（避免“空值可推断”）。
DIRECT_IDENTIFIER_TAGS = (
    "PatientName",
    "PatientBirthDate",
    "AccessionNumber",
)

# 需要重新分配 UID 的主干标签。只覆盖需求指定的 Study/Series/SOP。
REASSIGNED_UID_TAGS = (
    "StudyInstanceUID",
    "SeriesInstanceUID",
    "SOPInstanceUID",
)

# 序列中指向实例/序列/检查的引用标签：目标在本批内时必须同步改写，
# 目标在本批外时必须拒绝发布。这里只列“指向实例类”的引用；
# 类 UID（SOPClassUID 等）与 FrameOfReferenceUID 不在改写范围内。
REFERENCE_UID_TAGS = frozenset({
    "ReferencedSOPInstanceUID",
    "ReferencedSOPInstanceUIDInFile",
    "ReferencedSeriesInstanceUID",
    "ReferencedStudyInstanceUID",
})


class DedupError(Exception):
    """去标识无法安全完成（如引用指向批外实例）——拒绝发布。"""


# ------------------------------------------------------------------ 稳定映射
def _digest(batch_id: str, kind: str, value: str) -> str:
    raw = f"{batch_id}\x00{kind}\x00{value}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def uid_for(batch_id: str, old_uid: str, uid_root: str) -> str:
    """由 ``(批 ID, 旧 UID)`` 确定性地生成新 UID。

    同批重试 => 相同输入 => 相同输出；不同批 => 不同输出。
    结果只含十进制数字段，并保证不超过 DICOM 的 64 字符上限。
    """
    number = int(_digest(batch_id, "uid", old_uid)[:26], 16)
    new_uid = f"{uid_root}.{number}"
    if len(new_uid) > 64:
        raise DedupError(
            f"ANON_UID_ROOT 过长，生成的 UID 超过 64 字符：{new_uid}")
    return new_uid


def patient_id_for(batch_id: str, old_patient_id: str, prefix: str) -> str:
    """同批稳定的新 PatientID（不含任何原始标识片段）。"""
    digest = _digest(batch_id, "patient", old_patient_id or "")
    return f"{prefix}-{digest[:16].upper()}"


def _sanitize_filename_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", value)


# ------------------------------------------------------------------ 递归遍历
def _walk_dataset(ds: Dataset):
    """先序遍历数据集元素树（含嵌套序列）。"""
    for element in ds:
        yield element
        if element.VR == "SQ" and isinstance(element.value, Sequence):
            for item in element.value:
                if isinstance(item, Dataset):
                    yield from _walk_dataset(item)


def _strip_private(ds: Dataset) -> None:
    """递归删除所有私有标签（含私有创建者块与其数据元素）。"""
    for element in list(ds):
        if element.VR == "SQ" and isinstance(element.value, Sequence):
            for item in element.value:
                if isinstance(item, Dataset):
                    _strip_private(item)
    for tag in [e.tag for e in ds if e.tag.is_private]:
        del ds[tag]


def _strip_direct_identifiers(ds: Dataset) -> None:
    """递归删除 PatientName / PatientBirthDate / AccessionNumber。"""
    for element in list(ds):
        if element.VR == "SQ" and isinstance(element.value, Sequence):
            for item in element.value:
                if isinstance(item, Dataset):
                    _strip_direct_identifiers(item)
    for keyword in DIRECT_IDENTIFIER_TAGS:
        if keyword in ds:
            del ds[keyword]


def _replace_patient_ids(ds: Dataset, new_patient_id: str) -> None:
    """递归把 PatientID 替换为新值（含嵌套序列）。"""
    for element in _walk_dataset(ds):
        if element.tag.is_private or element.VR == "SQ":
            continue
        if element.keyword == "PatientID":
            element.value = new_patient_id


def _iter_references(ds: Dataset):
    """迭代数据集（含嵌套序列）中的实例引用 ``(Tag, 值)``。"""
    for element in _walk_dataset(ds):
        if element.tag.is_private or element.VR == "SQ":
            continue
        if element.keyword in REFERENCE_UID_TAGS and element.VR == "UI":
            value = str(element.value or "")
            if value:
                yield element.tag, value


def _rewrite_references(ds: Dataset, mapping: dict[str, str]) -> None:
    """把序列内指向本批实例的引用 UID 改写为新值。"""
    for element in _walk_dataset(ds):
        if element.tag.is_private or element.VR == "SQ":
            continue
        if element.keyword in REFERENCE_UID_TAGS and element.VR == "UI":
            new_value = mapping.get(str(element.value))
            if new_value is not None:
                element.value = new_value


# ------------------------------------------------------------------ 主流程
@dataclass
class PreparedInstance:
    """去标识后的内存副本。"""

    original_sop_uid: str
    new_sop_uid: str
    dataset: Dataset
    transfer_syntax: str
    file_name: str


@dataclass
class Plan:
    """一次去标识的完整计划。"""

    batch_id: str
    study_uid: str          # 新的 StudyInstanceUID
    patient_id: str         # 新的 PatientID
    instances: list[PreparedInstance] = field(default_factory=list)
    uid_mapping: dict[str, str] = field(default_factory=dict)


def _ensure_file_meta(ds: Dataset) -> None:
    if not hasattr(ds, "file_meta") or ds.file_meta is None:
        ds.file_meta = pydicom.dataset.FileMetaDataset()


def anonymized_file_name(ds: Dataset, sop_uid: str) -> str:
    """新文件名：只包含新 UID，不携带任何原始标识片段。"""
    series = _sanitize_filename_part(str(getattr(ds, "SeriesInstanceUID", "")))
    sop = _sanitize_filename_part(sop_uid)
    return f"{series}__{sop}.dcm" if series else f"{sop}.dcm"


def prepare_datasets(
    batch_id: str,
    datasets: list[tuple[str, Dataset]],
    *,
    uid_root: str,
    patient_prefix: str,
) -> Plan:
    """对已读入的一批数据集执行去标识，返回可写盘的 :class:`Plan`。

    ``datasets`` 元素为 ``(原始 SOPInstanceUID, Dataset)``。

    Raises
    ------
    DedupError
        当引用指向本批之外（或无法识别的目标）时拒绝发布，并说明原因。
    """
    if not datasets:
        raise DedupError("原包中没有任何 DICOM 实例，无法生成去标识包")

    # ---- 1. 递归删除私有标签（包含嵌套序列中的私有块）----
    for _, ds in datasets:
        _strip_private(ds)

    # ---- 2. 建立旧 UID -> 新 UID 映射（仅 Study/Series/SOP）----
    uid_mapping: dict[str, str] = {}
    seen_sop: set[str] = set()
    for orig_sop, ds in datasets:
        if orig_sop in seen_sop:
            raise DedupError(f"原包内 SOPInstanceUID 重复：{orig_sop}")
        seen_sop.add(orig_sop)
        values = [orig_sop]
        for keyword in ("StudyInstanceUID", "SeriesInstanceUID"):
            value = str(getattr(ds, keyword, "") or "")
            if value:
                values.append(value)
        for value in values:
            uid_mapping.setdefault(value, uid_for(batch_id, value, uid_root))

    # ---- 3. 批外引用检查：任何引用 UID 必须指向本批实例 ----
    problems: list[str] = []
    for orig_sop, ds in datasets:
        for tag, value in _iter_references(ds):
            if value not in uid_mapping:
                keyword = pydicom.datadict.keyword_for_tag(tag) or \
                    f"({tag.group:04X},{tag.element:04X})"
                problems.append(
                    f"实例 {orig_sop} 的 {keyword}={value} 指向本批之外的实例"
                )
    if problems:
        detail = "；".join(sorted(set(problems)))
        raise DedupError(
            "存在指向本批之外实例的引用，拒绝发布去标识包：" + detail
        )

    # ---- 4. 新 PatientID：同批内所有原始 PatientID 归并为同一个新值 ----
    original_patient_ids = {
        str(getattr(ds, "PatientID", "") or "") for _, ds in datasets
    }
    original_patient_ids.discard("")
    new_patient_id = patient_id_for(
        batch_id,
        sorted(original_patient_ids)[0] if original_patient_ids else "",
        patient_prefix,
    )

    new_study_uid = uid_mapping[str(datasets[0][1].StudyInstanceUID)]

    # ---- 5. 改写主 UID / 删除直接标识 / 改写引用 / 文件元信息 ----
    prepared: list[PreparedInstance] = []
    for orig_sop, ds in datasets:
        _strip_direct_identifiers(ds)
        _replace_patient_ids(ds, new_patient_id)

        _ensure_file_meta(ds)
        ts = str(getattr(ds.file_meta, "TransferSyntaxUID", "") or "")
        if not ts:
            raise DedupError(f"实例 {orig_sop} 缺少 TransferSyntaxUID")

        for keyword in REASSIGNED_UID_TAGS:
            if keyword in ds:
                old = str(ds.data_element(keyword).value)
                setattr(ds, keyword, uid_mapping[old])
        _rewrite_references(ds, uid_mapping)

        new_sop_uid = uid_mapping[orig_sop]
        ds.SOPInstanceUID = new_sop_uid
        ds.file_meta.MediaStorageSOPClassUID = ds.SOPClassUID
        ds.file_meta.MediaStorageSOPInstanceUID = new_sop_uid
        ds.file_meta.TransferSyntaxUID = pydicom.uid.UID(ts)

        prepared.append(PreparedInstance(
            original_sop_uid=orig_sop,
            new_sop_uid=new_sop_uid,
            dataset=ds,
            transfer_syntax=ts,
            file_name=anonymized_file_name(ds, new_sop_uid),
        ))

    prepared.sort(key=lambda item: item.original_sop_uid)
    return Plan(
        batch_id=batch_id,
        study_uid=new_study_uid,
        patient_id=new_patient_id,
        instances=prepared,
        uid_mapping=uid_mapping,
    )


def read_dicom_from_zip(zip_path: str) -> list[tuple[str, Dataset]]:
    """读取原 ZIP 中 ``dicom/`` 下的全部 DICOM 文件。"""
    import zipfile

    out: list[tuple[str, Dataset]] = []
    with zipfile.ZipFile(zip_path) as zf:
        names = sorted(
            name for name in zf.namelist()
            if name.startswith("dicom/") and not name.endswith("/")
            and name.lower().endswith(".dcm")
        )
        if not names:
            raise DedupError("原 ZIP 中没有 dicom/ 下的 .dcm 文件")
        for name in names:
            data = zf.read(name)
            ds = pydicom.dcmread(io.BytesIO(data), force=True)
            sop_uid = str(getattr(ds, "SOPInstanceUID", "") or "")
            if not sop_uid:
                raise DedupError(f"原 ZIP 内 {name} 缺少 SOPInstanceUID")
            out.append((sop_uid, ds))
    return out
