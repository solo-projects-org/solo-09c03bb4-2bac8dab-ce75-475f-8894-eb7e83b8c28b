"""定向去标识包：标签删除、UID 稳定性、引用改写、原子发布与重试。"""
from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path

import pydicom
import pytest
from pydicom.dataset import Dataset
from pydicom.sequence import Sequence

from app import anonymize, deid, storage
from app.config import Settings
from app.db import (
    DEID_COMPLETED,
    DEID_FAILED,
    Database,
    set_db,
)
from simulator.generate_data import (
    EXTERNAL_REFERENCE_SOP_UID,
    generate_study,
)

UID_ROOT = "1.2.826.0.1.3680043.10.999"


@pytest.fixture()
def workspace(tmp_path, monkeypatch):
    data = tmp_path / "data"
    monkeypatch.setattr(Settings, "DATA_DIR", data)
    monkeypatch.setattr(Settings, "DB_PATH", data / "retrieval.db")
    monkeypatch.setattr(Settings, "BATCHES_DIR", data / "batches")
    monkeypatch.setattr(Settings, "ZIP_DIR", data / "zips")
    Settings.ensure_dirs()
    db = Database(Settings.DB_PATH)
    set_db(db)
    yield data, db
    db.close()


def _make_source_zip(data: Path, study_index: int = 1) -> tuple[Path, list]:
    """用生成的虚构 CT 构造一个“原包”，并登记一个 COMPLETED 批。"""
    src = data / f"src{study_index}"
    meta = generate_study(
        src, study_index, 3, external_reference=(study_index == 4))
    files = sorted(Path(meta["directory"]).glob("*.dcm"))

    db = set_db
    from app.db import get_db
    db = get_db()
    batch_id = f"b{study_index}"
    db.create_batch(batch_id, meta["study_instance_uid"], "")
    db.transition_state(batch_id, ["PENDING"], "FINDING")
    db.set_manifest(batch_id, [
        (str(pydicom.dcmread(str(f), stop_before_pixels=True).SOPInstanceUID),
         "1.2.840.10008.5.1.4.1.1.2", "1.2.840.10008.1.2.1")
        for f in files
    ])
    db.transition_state(batch_id, ["FINDING"], "MOVING")
    for f in files:
        ds = pydicom.dcmread(str(f))
        path = storage.write_dataset(batch_id, ds, "1.2.840.10008.1.2.1")
        db.register_instance(batch_id, str(ds.SOPInstanceUID), str(path),
                             "1.2.840.10008.1.2.1")
    zip_path = storage.build_zip(batch_id)
    db.complete_batch(batch_id, str(zip_path))
    return zip_path, files


def _read_zip_dicom(zip_path: Path) -> list[pydicom.Dataset]:
    out = []
    with zipfile.ZipFile(zip_path) as zf:
        for name in sorted(n for n in zf.namelist() if n.startswith("dicom/")):
            out.append(pydicom.dcmread(io.BytesIO(zf.read(name))))
    return out


# ---------------------------------------------------------------- core
def test_removes_private_and_direct_identifiers_and_reassigns_uids(tmp_path):
    src = tmp_path / "src"
    meta = generate_study(src, 1, 3)
    files = sorted(Path(meta["directory"]).glob("*.dcm"))
    datasets = []
    for f in files:
        ds = pydicom.dcmread(str(f))
        datasets.append((str(ds.SOPInstanceUID), ds))

    plan = anonymize.prepare_datasets(
        "batch-1", datasets, uid_root=UID_ROOT, patient_prefix="ANON")

    old_uids = {old for old, _ in datasets}
    assert plan.study_uid not in old_uids
    assert plan.study_uid.startswith(UID_ROOT + ".")
    assert plan.patient_id.startswith("ANON-")

    new_sops = set()
    new_series = set()
    for item in plan.instances:
        ds = item.dataset
        # 直接标识已删除
        assert "PatientName" not in ds
        assert "PatientBirthDate" not in ds
        assert "AccessionNumber" not in ds
        assert str(ds.PatientID) == plan.patient_id
        # 私有标签（含嵌套序列）全部清除
        assert not any(e.tag.is_private for e in anonymize._walk_dataset(ds))
        # UID 已换新
        assert str(ds.SOPInstanceUID) == item.new_sop_uid
        assert item.new_sop_uid not in old_uids
        assert str(ds.file_meta.MediaStorageSOPInstanceUID) == item.new_sop_uid
        assert str(ds.StudyInstanceUID) == plan.study_uid
        new_sops.add(str(ds.SOPInstanceUID))
        new_series.add(str(ds.SeriesInstanceUID))
        # 文件名只含新 UID
        assert all(old not in item.file_name for old in old_uids)

    assert len(new_sops) == 3
    assert len(new_series) == 1

    # 序列内引用全部改写为批内新 SOP，且不再含旧 UID
    all_new = new_sops | {plan.study_uid} | new_series
    for item in plan.instances:
        ds = item.dataset
        refs = [str(i.ReferencedSOPInstanceUID)
                for i in ds.ReferencedImageSequence]
        assert refs and all(r in all_new for r in refs)
        for ref in refs:
            assert ref not in old_uids

    # 像素与几何标签保持原值
    before = pydicom.dcmread(str(files[0]))
    after = plan.instances[0].dataset
    for keyword in ("Rows", "Columns", "PixelSpacing", "SliceThickness",
                    "ImagePositionPatient", "ImageOrientationPatient",
                    "RescaleIntercept", "RescaleSlope", "SliceLocation"):
        assert getattr(after, keyword) == getattr(before, keyword), keyword
    assert after.PixelData == before.PixelData


def test_uid_reassignment_is_stable_across_retries(tmp_path):
    src = tmp_path / "src"
    meta = generate_study(src, 1, 2)

    def _plan(batch_id):
        datasets = []
        for f in sorted(Path(meta["directory"]).glob("*.dcm")):
            ds = pydicom.dcmread(str(f))
            datasets.append((str(ds.SOPInstanceUID), ds))
        return anonymize.prepare_datasets(
            batch_id, datasets, uid_root=UID_ROOT, patient_prefix="ANON")

    first = _plan("same-batch")
    second = _plan("same-batch")
    assert first.uid_mapping == second.uid_mapping
    assert first.patient_id == second.patient_id
    assert [i.new_sop_uid for i in first.instances] == \
        [j.new_sop_uid for j in second.instances]
    assert [i.file_name for i in first.instances] == \
        [j.file_name for j in second.instances]

    # 不同批 => 不同新值
    other = _plan("other-batch")
    assert other.study_uid != first.study_uid
    assert other.patient_id != first.patient_id


def test_external_reference_is_rejected_with_reason(tmp_path):
    src = tmp_path / "src"
    meta = generate_study(src, 1, 2)
    files = sorted(Path(meta["directory"]).glob("*.dcm"))
    datasets = []
    for f in files:
        ds = pydicom.dcmread(str(f))
        datasets.append((str(ds.SOPInstanceUID), ds))

    # 注入一个指向批外实例的引用
    external = Dataset()
    external.ReferencedSOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
    external.ReferencedSOPInstanceUID = EXTERNAL_REFERENCE_SOP_UID
    datasets[0][1].ReferencedImageSequence = Sequence([external])

    with pytest.raises(anonymize.DedupError) as exc:
        anonymize.prepare_datasets(
            "batch-x", datasets, uid_root=UID_ROOT, patient_prefix="ANON")
    message = str(exc.value)
    assert EXTERNAL_REFERENCE_SOP_UID in message
    assert "本批之外" in message
    assert "ReferencedSOPInstanceUID" in message


def test_nested_sequence_external_reference_is_also_rejected(tmp_path):
    """嵌套序列（如 SourceImageSequence 内再嵌的引用）同样必须检查。"""
    src = tmp_path / "src"
    meta = generate_study(src, 1, 2)
    files = sorted(Path(meta["directory"]).glob("*.dcm"))
    datasets = []
    for f in files:
        ds = pydicom.dcmread(str(f))
        datasets.append((str(ds.SOPInstanceUID), ds))

    inner = Dataset()
    inner.ReferencedSOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
    inner.ReferencedSOPInstanceUID = "1.2.826.0.1.3680043.8.498.555555"
    wrapper = Dataset()
    wrapper.ReferencedSOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
    wrapper.ReferencedSOPInstanceUID = datasets[1][0]  # 批内，合法
    wrapper.SourceImageSequence = Sequence([inner])
    datasets[0][1].SourceImageSequence = Sequence([wrapper])

    with pytest.raises(anonymize.DedupError) as exc:
        anonymize.prepare_datasets(
            "batch-y", datasets, uid_root=UID_ROOT, patient_prefix="ANON")
    assert "555555" in str(exc.value)


def test_geometry_tags_are_untouched(tmp_path):
    src = tmp_path / "src"
    meta = generate_study(src, 1, 2)
    files = sorted(Path(meta["directory"]).glob("*.dcm"))
    before = [pydicom.dcmread(str(f)) for f in files]
    datasets = [(str(ds.SOPInstanceUID), ds) for ds in before]
    plan = anonymize.prepare_datasets(
        "batch-z", datasets, uid_root=UID_ROOT, patient_prefix="ANON")
    for old, item in zip(before, plan.instances):
        new = item.dataset
        assert new.PixelData == old.PixelData
        for keyword in ("Rows", "Columns", "PixelSpacing", "SliceThickness",
                        "SliceLocation", "ImagePositionPatient",
                        "ImageOrientationPatient", "RescaleIntercept",
                        "RescaleSlope", "InstanceNumber", "SeriesNumber"):
            assert getattr(new, keyword) == getattr(old, keyword), keyword


# ---------------------------------------------------------------- packaging
def test_generate_publishes_atomically_and_is_idempotent(workspace):
    data, db = workspace
    zip_path, files = _make_source_zip(data)
    batch_id = "b1"
    original_copy = zip_path.read_bytes()

    result = deid.generate(db, batch_id)
    assert result.state == DEID_COMPLETED, result
    final = Path(result.zip_path)
    assert final.exists()
    assert final.name == f"{batch_id}.deid.zip"
    # 无临时残留
    assert not list(Settings.ZIP_DIR.glob("*.deid.zip.part"))

    # 原包不受影响
    assert zip_path.read_bytes() == original_copy

    # 幂等：再次调用返回同一路径，不重复写盘
    before_mtime = final.stat().st_mtime_ns
    again = deid.generate(db, batch_id)
    assert again.state == DEID_COMPLETED
    assert again.zip_path == result.zip_path
    assert final.stat().st_mtime_ns == before_mtime

    # 新包内容：交付物只含新 UID；旧 -> 新映射仅保留在服务端
    with zipfile.ZipFile(final) as zf:
        names = zf.namelist()
        manifest = zf.read("MANIFEST.csv").decode().strip().splitlines()
        deid_info = zf.read("DEID_INFO.txt").decode()
        assert "DEID_INFO.txt" in names
    assert len(manifest) == 4  # 表头 + 3
    assert manifest[0] == "sop_instance_uid,file_name"
    manifest_sops = {line.split(",")[0] for line in manifest[1:]}
    with zipfile.ZipFile(final) as zf:
        dicom_names = [n for n in zf.namelist() if n.startswith("dicom/")]
    assert manifest_sops == {
        n.removeprefix("dicom/").split("__")[-1][:-4] for n in dicom_names
    }
    assert "original" not in deid_info
    assert "batch_id" not in deid_info
    # 服务端保留映射（可用于审计/回溯，但不出现在交付包内）
    mapping = db.deid_instances(batch_id)
    assert len(mapping) == 3
    assert {row["new_sop_uid"] for row in mapping} == manifest_sops

    for ds in _read_zip_dicom(final):
        assert "PatientName" not in ds
        assert "PatientBirthDate" not in ds
        assert "AccessionNumber" not in ds
        assert not any(e.tag.is_private for e in anonymize._walk_dataset(ds))


def test_external_reference_never_publishes(workspace):
    data, db = workspace
    zip_path, _ = _make_source_zip(data, study_index=4)
    result = deid.generate(db, "b4")
    assert result.state == DEID_FAILED
    assert "本批之外" in (result.error or "")
    assert not deid.deid_zip_path("b4").exists()
    assert not list(Settings.ZIP_DIR.glob("*.deid.zip.part"))
    # 失败可重试：再调用仍是 FAILED，但不会暴露半成品
    again = deid.generate(db, "b4")
    assert again.state == DEID_FAILED
    assert again.attempt == 2


def test_concurrent_generate_publishes_single_complete_package(workspace):
    import threading

    data, db = workspace
    _make_source_zip(data)
    results: list = []
    errors: list = []
    lock = threading.Lock()

    def worker():
        try:
            r = deid.generate(db, "b1", wait_timeout=30)
            with lock:
                results.append(r)
        except Exception as exc:  # pragma: no cover
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not errors
    assert len(results) == 8
    assert all(r.state == DEID_COMPLETED for r in results)
    # 全部指向同一份完整包
    assert len({r.zip_path for r in results}) == 1
    assert not list(Settings.ZIP_DIR.glob("*.deid.zip.part"))
    final = Path(results[0].zip_path)
    with zipfile.ZipFile(final) as zf:
        assert len([n for n in zf.namelist() if n.startswith("dicom/")]) == 3


def test_write_failure_leaves_no_partial_package_and_can_retry(
    workspace, monkeypatch
):
    data, db = workspace
    _make_source_zip(data)

    calls = {"n": 0}
    real_replace = os.replace

    def flaky_replace(src, dst):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("模拟写盘失败")
        return real_replace(src, dst)

    monkeypatch.setattr(deid.os, "replace", flaky_replace)

    first = deid.generate(db, "b1")
    assert first.state == DEID_FAILED
    assert "写盘/发布失败" in (first.error or "")
    assert not deid.deid_zip_path("b1").exists()
    assert not list(Settings.ZIP_DIR.glob("*.deid.zip.part"))

    # 重试成功，且 UID 与失败那次一致（同批稳定）
    second = deid.generate(db, "b1")
    assert second.state == DEID_COMPLETED, second
    assert second.attempt == 2
    assert Path(second.zip_path).exists()


def test_restart_recovery_converts_running_to_failed_and_cleans_parts(
    workspace,
):
    data, db = workspace
    _make_source_zip(data)

    # 模拟服务在发布前被杀：留下 RUNNING 与临时包
    assert db.claim_deid("b1")
    part = Settings.ZIP_DIR / "b1.deid.zip.part"
    part.write_bytes(b"partial")

    recovered = deid.recover_on_startup(db)
    assert "b1" in recovered
    row = db.get_deid("b1")
    assert row["state"] == DEID_FAILED
    assert "重启" in row["error"]
    assert not part.exists()
    # 未暴露任何最终包
    assert not deid.deid_zip_path("b1").exists()

    # 重启后重试可发布
    result = deid.generate(db, "b1")
    assert result.state == DEID_COMPLETED
    assert Path(result.zip_path).exists()


def test_published_package_survives_restart(workspace):
    data, db = workspace
    _make_source_zip(data)
    result = deid.generate(db, "b1")
    assert result.state == DEID_COMPLETED
    path = result.zip_path

    deid.recover_on_startup(db)  # 等价于服务重启
    row = db.get_deid("b1")
    assert row["state"] == DEID_COMPLETED
    assert row["zip_path"] == path
    assert Path(path).exists()


def test_orphan_final_package_is_removed_on_restart(workspace):
    """改名成功后、登记前被杀：最终文件存在但 DB 无 COMPLETED 记录。"""
    data, db = workspace
    _make_source_zip(data)
    assert db.claim_deid("b1")
    final = deid.deid_zip_path("b1")
    final.write_bytes(b"published-but-unregistered")

    deid.recover_on_startup(db)
    assert not final.exists(), "未登记包不能被当作已发布暴露"
    assert db.get_deid("b1")["state"] == DEID_FAILED

    retried = deid.generate(db, "b1")
    assert retried.state == DEID_COMPLETED


def test_missing_published_file_becomes_retryable(workspace):
    data, db = workspace
    _make_source_zip(data)
    result = deid.generate(db, "b1")
    Path(result.zip_path).unlink()

    deid.recover_on_startup(db)
    assert db.get_deid("b1")["state"] == DEID_FAILED
    retried = deid.generate(db, "b1")
    assert retried.state == DEID_COMPLETED
