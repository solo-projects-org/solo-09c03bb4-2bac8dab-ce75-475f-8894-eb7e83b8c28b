"""生成完全虚构的合成 CT 数据集（无任何真实患者信息）。

每个实例都是合法的 DICOM CT Image Storage 对象，未压缩传输语法，
可被 pydicom/pynetdicom 直接读写与网络传输。
"""
from __future__ import annotations

import argparse
import math
from pathlib import Path

import pydicom
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.sequence import Sequence
from pydicom.uid import (
    CTImageStorage,
    ExplicitVRLittleEndian,
    generate_uid,
)

# 固定 UID 前缀，保证演示环境内 UID 稳定可读
UID_PREFIX = "1.2.826.0.1.3680043.8.498."
PATIENT_NAMES = {
    1: "PHANTOM^Alpha",
    2: "PHANTOM^Bravo",
    3: "PHANTOM^Charlie",
}
# 虚构私有创建者（模拟院内在设备/后处理站写入的私有标签块）
PRIVATE_CREATOR = "SYNTH_PRIV_01"

# 批外引用演示 Study 的固定 UID：该实例“引用”了一个不属于本批的检查
EXTERNAL_REFERENCE_STUDY_UID = f"{UID_PREFIX}777001"
EXTERNAL_REFERENCE_SOP_UID = f"{UID_PREFIX}777002"
# 额外虚构检查（索引 4）：用于演示“引用批外实例 -> 拒绝发布”
EXTERNAL_STUDY_INDEX = 4


def _make_uid(serial: int) -> str:
    return f"{UID_PREFIX}{serial}"


def build_ct_dataset(
    study_uid: str,
    series_uid: str,
    sop_uid: str,
    patient_id: str,
    patient_name: str,
    instance_number: int,
    instance_count: int,
    rows: int = 64,
    cols: int = 64,
    private_tags: bool = False,
    referenced_uids: list[str] | None = None,
) -> Dataset:
    """构造一个内存中的合成 CT Dataset（带 file_meta）。

    ``private_tags``：写入虚构私有创建者块与私有元素（含嵌套序列里的私有
    标签），用于演示“递归删除私有标签”。

    ``referenced_uids``：在 ``ReferencedImageSequence`` 中写入这些
    ``ReferencedSOPInstanceUID``，用于演示“序列内引用同步改写”。
    """
    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = str(CTImageStorage)
    file_meta.MediaStorageSOPInstanceUID = sop_uid
    file_meta.TransferSyntaxUID = str(ExplicitVRLittleEndian)
    file_meta.ImplementationClassUID = _make_uid(9900)
    file_meta.ImplementationVersionName = "SIMPACS_1"

    ds = Dataset()
    ds.file_meta = file_meta
    ds.preamble = b"\0" * 128

    ds.SOPClassUID = str(CTImageStorage)
    ds.SOPInstanceUID = sop_uid
    ds.StudyInstanceUID = study_uid
    ds.SeriesInstanceUID = series_uid

    ds.PatientName = patient_name
    ds.PatientID = patient_id
    ds.PatientBirthDate = "19700101"
    ds.PatientSex = "O"

    ds.Modality = "CT"
    ds.StudyDate = "20261101"
    ds.StudyTime = "090000"
    ds.ContentDate = "20261101"
    minute = (instance_number - 1) % 60
    ds.ContentTime = f"09{minute:02d}00"
    ds.AccessionNumber = f"SIM{patient_id}"
    ds.StudyID = "S001"
    # 描述不携带患者片段：定向去标识只处理明文标识字段，
    # 演示数据不应把标识藏进自由文本描述里。
    ds.StudyDescription = "Synthetic axial CT study (demo)"
    ds.SeriesNumber = 1
    ds.SeriesDescription = "Synthetic axial CT"
    ds.InstanceNumber = instance_number

    ds.ImageType = ["ORIGINAL", "PRIMARY", "AXIAL"]
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.SamplesPerPixel = 1
    ds.BitsAllocated = 16
    ds.BitsStored = 16
    ds.HighBit = 15
    ds.PixelRepresentation = 0
    ds.Rows = rows
    ds.Columns = cols
    ds.PixelSpacing = [1.0, 1.0]
    ds.SliceThickness = 2.0
    ds.SliceLocation = (instance_number - 1 - instance_count / 2) * 2.0
    ds.ImagePositionPatient = [0.0, 0.0, float(ds.SliceLocation)]
    ds.ImageOrientationPatient = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0]
    ds.RescaleIntercept = -1024
    ds.RescaleSlope = 1
    ds.RescaleType = "HU"
    ds.DeviceSerialNumber = "SYNTH-001"
    ds.Manufacturer = "Synthetic PACS Demo"

    # ---- 序列内引用（指向本批其它实例，去标识时必须同步改写）----
    if referenced_uids:
        refs = []
        for ref_uid in referenced_uids:
            item = Dataset()
            item.ReferencedSOPClassUID = str(CTImageStorage)
            item.ReferencedSOPInstanceUID = ref_uid
            refs.append(item)
        ds.ReferencedImageSequence = Sequence(refs)
        ds.SourceImageSequence = Sequence(refs)

    # ---- 虚构私有标签（模拟设备/后处理站写入，去标识时须递归删除）----
    if private_tags:
        ds.add_new((0x0009, 0x0010), "LO", PRIVATE_CREATOR)   # 私有创建者
        ds.add_new((0x0009, 0x1001), "LO", f"RAW-{patient_name}")
        ds.add_new((0x0009, 0x1002), "DS", "123.456")
        # 嵌套序列里的私有标签：验证“递归”删除
        inner = Dataset()
        inner.add_new((0x0009, 0x0010), "LO", PRIVATE_CREATOR + "_INNER")
        inner.add_new((0x0009, 0x1003), "LO", f"INNER-{patient_id}")
        inner.CodeValue = "99SYNTH"
        inner.CodingSchemeDesignator = "99SYNTH"
        inner.CodeMeaning = "private nested content"
        nested = Dataset()
        nested.RequestedProcedureID = f"RP{instance_number:03d}"
        nested.add_new((0x0009, 0x0010), "LO", PRIVATE_CREATOR + "_DEEP")
        nested.add_new((0x0009, 0x1004), "LO", "DEEP-PRIVATE-VALUE")
        nested.ReferencedImageSequence = Sequence([
            _ref_item(referenced_uids[0]) if referenced_uids else _ref_item(sop_uid)
        ])
        inner.ConceptNameCodeSequence = Sequence([nested])
        ds.ProcedureCodeSequence = Sequence([inner])

    # 确定性的“模体”像素：径向渐变 + 层间变化，HU 值在常见 CT 范围内
    pixels = bytearray()
    cx = cy = (rows - 1) / 2.0
    for r in range(rows):
        for c in range(cols):
            dist = math.sqrt((r - cy) ** 2 + (c - cx) ** 2)
            hu = int(400 * math.exp(-(dist ** 2) / 200.0)
                     + 50 * math.sin(instance_number + dist / 4) - 200)
            hu = max(-1024, min(3071, hu))
            pixels += int(hu + 1024).to_bytes(2, "little", signed=False)
    ds.PixelData = bytes(pixels)
    return ds


def _ref_item(ref_uid: str) -> Dataset:
    item = Dataset()
    item.ReferencedSOPClassUID = str(CTImageStorage)
    item.ReferencedSOPInstanceUID = ref_uid
    return item


def generate_study(
    out_dir: Path,
    study_index: int,
    instance_count: int,
    *,
    external_reference: bool = False,
) -> dict:
    """生成一个检查的全部实例文件，返回检查元数据。

    每个实例引用前一个实例（首实例引用第二个实例），私有标签块包含
    创建者、私有元素以及嵌套序列内的私有标签，用于演示去标识。
    ``external_reference=True`` 时，首个实例额外引用一个**批外**检查，
    用于演示“引用指向批外实例 -> 拒绝发布”。
    """
    patient_id = f"P{study_index:03d}"
    study_uid = (
        EXTERNAL_REFERENCE_STUDY_UID if external_reference
        else _make_uid(1000 + study_index)
    )
    series_uid = _make_uid(2000 + study_index)
    study_dir = out_dir / study_uid
    study_dir.mkdir(parents=True, exist_ok=True)

    sop_uids = [
        _make_uid(3000 + study_index * 100 + i)
        for i in range(1, instance_count + 1)
    ]
    for i in range(1, instance_count + 1):
        sop_uid = sop_uids[i - 1]
        refs = []
        if instance_count > 1:
            refs.append(sop_uids[i % instance_count])  # 序列内互引
        if external_reference and i == 1:
            refs.append(EXTERNAL_REFERENCE_SOP_UID)   # 批外引用
        ds = build_ct_dataset(
            study_uid=study_uid,
            series_uid=series_uid,
            sop_uid=sop_uid,
            patient_id=patient_id,
            patient_name=PATIENT_NAMES.get(
                study_index, f"PHANTOM^{study_index}"),
            instance_number=i,
            instance_count=instance_count,
            private_tags=True,
            referenced_uids=refs,
        )
        ds.save_as(str(study_dir / f"{i:04d}.dcm"), enforce_file_format=True)

    return {
        "study_instance_uid": study_uid,
        "series_instance_uid": series_uid,
        "patient_id": patient_id,
        "patient_name": PATIENT_NAMES.get(
            study_index, f"PHANTOM^{study_index}"),
        "instances": instance_count,
        "directory": str(study_dir),
        "has_external_reference": external_reference,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="生成虚构合成 CT 数据")
    parser.add_argument("--out", required=True, help="输出目录")
    parser.add_argument(
        "--counts", default="8,6,5,2",
        help="虚构检查各自的实例数，逗号分隔（默认 8,6,5,2；"
             "第 4 个检查含批外引用，用于演示拒绝发布）",
    )
    args = parser.parse_args()

    out_dir = Path(args.out)
    counts = [int(x) for x in args.counts.split(",")]
    manifest = []
    for idx, count in enumerate(counts, start=1):
        external = idx == EXTERNAL_STUDY_INDEX
        meta = generate_study(
            out_dir, idx, count, external_reference=external
        )
        manifest.append(meta)
        note = "（含批外引用，去标识应拒绝发布）" if external else ""
        print(f"generated study {meta['study_instance_uid']} "
              f"({count} instances){note} -> {meta['directory']}")

    import json
    manifest_path = out_dir / "studies.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
    print(f"manifest -> {manifest_path}")


if __name__ == "__main__":
    main()
