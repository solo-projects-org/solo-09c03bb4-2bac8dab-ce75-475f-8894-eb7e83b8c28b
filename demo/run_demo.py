"""端到端演示编排（在 api 容器内或任何能访问 API/PACS 的环境运行）。

演示序列
========
1. 列出模拟 PACS 中的虚构 CT 检查
2. 并行提交两个正常批（交错/慢回传互不阻塞，其中一个启用源端重复投送）
3. 回传进行中投送“清单外实例”和“压缩实例”，验证均被拒收、不入包
4. 提交一个 save_failure 批，验证保存失败即批失败、C-STORE 回失败码
5. 轮询进度，下载完成批 ZIP 并校验实例数与清单一致
6. 重启恢复演示：制造一个未完成批后调用进程退出，验证重启后该批判失败、
   已完成批仍可下载

运行::

    python -m demo.run_demo            # 完整演示
    python -m demo.run_demo --skip-restart
"""
from __future__ import annotations

import argparse
import io
import os
import sys
import time
import zipfile

import requests

# 默认走 compose 服务名；本地裸跑时可用环境变量覆盖为 127.0.0.1
API = os.getenv("API_BASE", "http://api:8000")
HOST_API = os.getenv("STORE_HOST", "api")
PORT_API = int(os.getenv("STORE_PORT", "11113"))


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def wait_state(batch_id: str, target: set[str], timeout: float = 180) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = requests.get(f"{API}/batches/{batch_id}", timeout=10)
        r.raise_for_status()
        view = r.json()
        if view["state"] in target:
            return view
        print(f"    ... {batch_id[:8]} {view['state']} "
              f"{view['received_count']}/{view['expected_count']}")
        time.sleep(1.5)
    raise TimeoutError(f"批 {batch_id} 未在 {timeout}s 内到达 {target}")


def show_events(batch_id: str) -> None:
    events = requests.get(f"{API}/batches/{batch_id}/events",
                          timeout=10).json()
    for e in events:
        print(f"    [{e['created_at'][11:]}] {e['kind']}: {e['message']}")


def _create_batch(study_uid: str) -> str:
    r = requests.post(f"{API}/batches",
                      json={"study_instance_uid": study_uid}, timeout=10)
    r.raise_for_status()
    return r.json()["id"]


def demo_deid_package(study_uid: str, view: dict) -> None:
    """完整走一遍：已有完成批 -> 生成去标识包 -> 校验 -> 下载。"""
    if view["state"] != "COMPLETED":
        print("    批 A 未完成，跳过去标识演示")
        return
    import pydicom

    bid = view["id"]
    original = requests.get(f"{API}/batches/{bid}/download", timeout=30).content
    old_zf = zipfile.ZipFile(io.BytesIO(original))
    old_sops = {
        str(pydicom.dcmread(io.BytesIO(old_zf.read(n))).SOPInstanceUID)
        for n in old_zf.namelist() if n.startswith("dicom/")
    }

    # 同批并发生成：只发布一份完整新包，所有请求得到同一结果
    import threading
    responses: list = []
    lock = threading.Lock()

    def _post() -> None:
        r = requests.post(f"{API}/batches/{bid}/deid-package", timeout=120)
        with lock:
            responses.append(r)

    threads = [threading.Thread(target=_post) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    codes = [r.status_code for r in responses]
    print(f"    并发 4 个生成请求 -> HTTP={codes}")
    assert all(c in (200, 201) for c in codes), codes
    assert sum(c == 201 for c in codes) == 1, codes
    assert len({r.json()["new_study_instance_uid"] for r in responses}) == 1
    resp = next(r for r in responses if r.status_code == 201)
    print("    只发布一份完整新包：首次 201，其余 200 复用同一包")
    payload = resp.json()
    print(f"    新 StudyInstanceUID={payload['new_study_instance_uid']}")
    print(f"    新 PatientID={payload['new_patient_id']}")
    print(f"    实例数={payload['instance_count']}，attempt={payload['attempt']}")

    # 幂等重试：同一份包，同一批 UID
    again = requests.post(f"{API}/batches/{bid}/deid-package", timeout=120)
    print(f"    重复生成 HTTP={again.status_code}（期望 200，复用同一完整包）")
    assert again.status_code == 200
    assert again.json()["new_study_instance_uid"] == payload["new_study_instance_uid"]

    dl = requests.get(f"{API}/batches/{bid}/deid-download", timeout=30)
    dl.raise_for_status()
    new_zf = zipfile.ZipFile(io.BytesIO(dl.content))
    names = [n for n in new_zf.namelist() if n.startswith("dicom/")]
    manifest = new_zf.read("MANIFEST.csv").decode().strip().splitlines()
    print(f"    新包 DICOM 文件数={len(names)}，MANIFEST 行数（含表头）={len(manifest)}")
    assert len(names) == view["expected_count"]
    assert len(manifest) == view["expected_count"] + 1

    new_sops = set()
    for name in names:
        ds = pydicom.dcmread(io.BytesIO(new_zf.read(name)))
        new_sops.add(str(ds.SOPInstanceUID))
        assert "PatientName" not in ds, "PatientName 未删除"
        assert "PatientBirthDate" not in ds, "PatientBirthDate 未删除"
        assert "AccessionNumber" not in ds, "AccessionNumber 未删除"
        assert not any(e.tag.is_private for e in ds), "残留私有标签"
        assert str(ds.StudyInstanceUID) == payload["new_study_instance_uid"]
        assert str(ds.file_meta.MediaStorageSOPInstanceUID) == str(ds.SOPInstanceUID)
        refs = [str(i.ReferencedSOPInstanceUID)
                for i in ds.ReferencedImageSequence]
        assert refs, "序列内引用丢失"
        for ref in refs:
            # 引用已与本批新 SOP 同步（处于同一映射代）
            assert ref not in old_sops, f"引用仍是旧 SOP：{ref}"
        # 文件名只含新 UID
        assert all(old not in name for old in old_sops), name
    assert not (new_sops & old_sops), "新 SOP 不得等于旧 SOP"

    # 文件名清单与包内一致
    manifest_sops = {line.split(",")[0] for line in manifest[1:]}
    assert manifest_sops == new_sops
    print("    校验通过：直接标识已删、私有标签递归清除、UID 换新、"
          "引用/文件名/清单同步改写")

    original_again = requests.get(
        f"{API}/batches/{bid}/download", timeout=30)
    print(f"    原包仍可下载 HTTP={original_again.status_code}（期望 200）")
    assert original_again.status_code == 200
    assert original_again.content == original, "原包被改写"
    print("    原包内容未受影响")


def demo_rejected_package(studies: list[dict]) -> None:
    """含批外引用的检查：去标识必须拒绝发布并给出原因。"""
    external = next(
        (s for s in studies
         if "777001" in s["study_instance_uid"]), None)
    if external is None:
        print("    演示数据中没有批外引用检查（第 4 个 Study），跳过")
        return
    bid = _create_batch(external["study_instance_uid"])
    view = wait_state(bid, {"COMPLETED", "FAILED"})
    print(f"    原批状态={view['state']} {view['received_count']}/"
          f"{view['expected_count']}")
    if view["state"] != "COMPLETED":
        print("    原批未完成，跳过去标识拒绝演示")
        return
    resp = requests.post(f"{API}/batches/{bid}/deid-package", timeout=120)
    print(f"    生成去标识包 HTTP={resp.status_code}（期望 422）")
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    print(f"    拒绝原因：{detail['message']}")
    assert "本批之外" in detail["message"]

    status = requests.get(f"{API}/batches/{bid}/deid-package", timeout=10).json()
    print(f"    状态={status['state']}，error={status['error']}")
    dl = requests.get(f"{API}/batches/{bid}/deid-download", timeout=10)
    print(f"    去标识包下载 HTTP={dl.status_code}（期望 409，不暴露任何包）")
    assert dl.status_code == 409
    show_events(bid)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-restart", action="store_true",
                        help="跳过重启恢复演示")
    args = parser.parse_args()

    banner("1. 列出模拟 PACS 中的检查")
    studies = requests.get(f"{API}/internal/pacs-studies", timeout=20).json()
    for s in studies:
        print(f"    Study={s['study_instance_uid']} 患者={s['patient_name']} "
              f"实例数={s['instances']}")
    if len(studies) < 3:
        print("PACS 至少需要 3 个检查")
        return 1
    study_a, study_b, study_c = (s["study_instance_uid"] for s in studies[:3])

    banner("2. 并行提交批 A（含源端重复投送）与批 B，验证交错回传互不阻塞")
    # 批 A 的重复投送由 PACS 环境变量 MOVE_DUPLICATE_EVERY=3 控制
    r_a = requests.post(f"{API}/batches",
                        json={"study_instance_uid": study_a}, timeout=10)
    r_a.raise_for_status()
    batch_a = r_a.json()["id"]
    r_b = requests.post(f"{API}/batches",
                        json={"study_instance_uid": study_b}, timeout=10)
    r_b.raise_for_status()
    batch_b = r_b.json()["id"]
    print(f"    批 A={batch_a}  Study={study_a}")
    print(f"    批 B={batch_b}  Study={study_b}")

    # 等清单固定、进入 MOVING，再投 rogue 流量
    time.sleep(3)

    banner("3. 清单外实例 / 压缩实例攻击，均应被拒收且不入包")
    from simulator import rogue_sender

    old_argv = sys.argv
    sys.argv = ["rogue", "off-list", "--study", study_a,
                "--host", HOST_API, "--port", str(PORT_API)]
    try:
        rc_offlist = rogue_sender.main()
    finally:
        sys.argv = old_argv
    sys.argv = ["rogue", "compressed", "--study", study_b,
                "--host", HOST_API, "--port", str(PORT_API)]
    try:
        rc_compressed = rogue_sender.main()
    finally:
        sys.argv = old_argv
    print(f"    清单外投送拒收校验返回码={rc_offlist}（0=符合预期）")
    print(f"    压缩投送拒收校验返回码={rc_compressed}（0=符合预期）")

    banner("4. 提交批 C 并注入保存失败（批目录只读，真实写盘失败）")
    r_c = requests.post(f"{API}/batches",
                        json={"study_instance_uid": study_c,
                              "fault_inject": "save_failure"}, timeout=10)
    r_c.raise_for_status()
    batch_c = r_c.json()["id"]
    print(f"    批 C={batch_c}（save_failure）")

    banner("5. 等待三个批到达终态")
    view_a = wait_state(batch_a, {"COMPLETED", "FAILED"})
    view_b = wait_state(batch_b, {"COMPLETED", "FAILED"})
    view_c = wait_state(batch_c, {"COMPLETED", "FAILED"}, timeout=120)
    print(f"    批 A 终态={view_a['state']} "
          f"{view_a['received_count']}/{view_a['expected_count']}")
    print(f"    批 B 终态={view_b['state']} "
          f"{view_b['received_count']}/{view_b['expected_count']}")
    print(f"    批 C 终态={view_c['state']} 原因={view_c['failure_reason']}")
    view_a["id"] = batch_a

    print("\n  -- 批 A 事件流水（可见重复投送去重、rogue 拒收）--")
    show_events(batch_a)
    print("\n  -- 批 C 事件流水（可见保存失败）--")
    show_events(batch_c)

    banner("6. 下载批 A 的 ZIP 并核对内容")
    if view_a["state"] != "COMPLETED":
        print("    批 A 未完成，跳过下载校验")
    else:
        resp = requests.get(f"{API}/batches/{batch_a}/download", timeout=30)
        resp.raise_for_status()
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        dicom_files = [n for n in zf.namelist() if n.startswith("dicom/")]
        manifest = zf.read("MANIFEST.csv").decode().strip().splitlines()
        print(f"    ZIP 内 DICOM 文件数={len(dicom_files)}，"
              f"清单行数（含表头）={len(manifest)}")
        assert len(dicom_files) == view_a["expected_count"], "ZIP 实例数与清单不符"
        assert len(manifest) == view_a["expected_count"] + 1
        print("    校验通过：ZIP 仅含清单内实例，数量一致")

    # 对失败批下载应被拒绝
    denied = requests.get(f"{API}/batches/{batch_c}/download", timeout=10)
    print(f"    失败批 C 下载 HTTP 状态={denied.status_code}（期望 409）")
    assert denied.status_code == 409

    banner("6b. 定向去标识包：生成、校验、下载（原包保持可下载）")
    demo_deid_package(study_a, view_a)

    banner("6c. 批外引用：拒绝发布并说明原因")
    demo_rejected_package(studies)

    if args.skip_restart:
        banner("演示完成（已跳过重启恢复）")
        return 0

    banner("7. 重启恢复：制造未完成批后重启进程")
    # 再提交一个新检查的批，立刻杀掉进程，使其停在非终态。
    # 终态批已存在不影响重新提交（仅拒绝“进行中”的同 Study 批）。
    r_d = requests.post(f"{API}/batches",
                        json={"study_instance_uid": study_b}, timeout=10)
    if r_d.status_code == 409:
        print("    存在同 Study 进行中批，重启恢复场景略过")
    else:
        r_d.raise_for_status()
        batch_d = r_d.json()["id"]
        print(f"    批 D={batch_d} 已提交，立即触发进程退出")
        try:
            requests.post(f"{API}/internal/suicide", timeout=5)
        except requests.RequestException:
            pass
        # 等待容器编排（compose restart: always）或外部守护拉起进程
        deadline = time.time() + 90
        healthy = False
        while time.time() < deadline:
            try:
                if requests.get(f"{API}/health", timeout=3).ok:
                    healthy = True
                    break
            except requests.RequestException:
                time.sleep(1.5)
        if not healthy:
            print("    服务未在 90s 内恢复（本地裸跑请手动重启 API 后重试）")
            return 2
        view_d = wait_state(batch_d, {"COMPLETED", "FAILED"})
        print(f"    重启后批 D 状态={view_d['state']} "
              f"原因={view_d['failure_reason']}")
        assert view_d["state"] == "FAILED"
        assert "重启" in (view_d["failure_reason"] or "")
        show_events(batch_d)
        # 已完成批仍可下载；若 ZIP 丢失也会在启动时重建
        if view_a["state"] == "COMPLETED":
            again = requests.get(f"{API}/batches/{batch_a}/download", timeout=30)
            print(f"    重启后已完成批 A 下载 HTTP 状态={again.status_code}（期望 200）")
            assert again.status_code == 200

    banner("全部演示完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())
