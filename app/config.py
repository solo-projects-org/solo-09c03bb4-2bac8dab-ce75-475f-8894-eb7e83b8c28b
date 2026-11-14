"""运行期配置，全部来自环境变量，容器内由 compose 注入。"""
from __future__ import annotations

import os
from pathlib import Path


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw not in (None, "") else default


class Settings:
    # ---- 本机（接收端 / 检索 SCU）----
    APP_AET: str = os.getenv("APP_AET", "RETRIEVER")
    APP_HOST: str = os.getenv("APP_HOST", "0.0.0.0")
    STORE_PORT: int = _int("STORE_PORT", 11113)
    # C-MOVE 的 Move Destination 只携带本 AE Title；源端 PACS 自行把
    # AE Title 解析为网络地址（模拟 PACS 用 DEST_HOST/DEST_PORT 配置）。

    # ---- 源端 PACS ----
    PACS_AET: str = os.getenv("PACS_AET", "MYPACS")
    PACS_HOST: str = os.getenv("PACS_HOST", "pacs")
    PACS_PORT: int = _int("PACS_PORT", 11112)

    # ---- DICOM 网络超时（秒）----
    ACSE_TIMEOUT: int = _int("ACSE_TIMEOUT", 30)
    DIMSE_TIMEOUT: int = _int("DIMSE_TIMEOUT", 120)
    NETWORK_TIMEOUT: int = _int("NETWORK_TIMEOUT", 120)

    # ---- 数据卷 ----
    DATA_DIR: Path = Path(os.getenv("DATA_DIR", "/data"))
    DB_PATH: Path = DATA_DIR / "retrieval.db"
    BATCHES_DIR: Path = DATA_DIR / "batches"
    ZIP_DIR: Path = DATA_DIR / "zips"

    # ---- 定向去标识包 ----
    # 重新分配 UID 时使用固定根；保证同批重试生成完全一致的新 UID。
    ANON_UID_ROOT: str = os.getenv(
        "ANON_UID_ROOT", "1.2.826.0.1.3680043.10.999"
    )
    # 新 PatientID 前缀（后接由批 ID 派生的稳定摘要）
    ANON_PATIENT_PREFIX: str = os.getenv("ANON_PATIENT_PREFIX", "ANON")

    @classmethod
    def ensure_dirs(cls) -> None:
        cls.DATA_DIR.mkdir(parents=True, exist_ok=True)
        cls.BATCHES_DIR.mkdir(parents=True, exist_ok=True)
        cls.ZIP_DIR.mkdir(parents=True, exist_ok=True)


settings = Settings()
