"""SQLite 持久化层。

设计要点
========
* 全部写入经同一把可重入锁串行化，配合 ``check-then-write`` 逻辑保证
  重复实例不重复计数、批状态迁移只发生一次。
* ``instances`` 表对 ``(batch_id, sop_instance_uid)`` 建唯一索引：
  清单登记和回传登记都靠它天然去重。
* 文件与登记在同一事务边界内处理：调用方先把文件写好并 fsync，再调用
  ``register_instance``；登记失败时调用方负责删除已写文件，保证
  “文件写完且登记成功才回复 C-STORE 成功”。
"""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

# ---- 批状态（终态不可变，迟到回传无法恢复失败批）----
STATE_PENDING = "PENDING"        # 已创建，尚未开始 C-FIND
STATE_FINDING = "FINDING"        # C-FIND 固定清单中
STATE_MOVING = "MOVING"          # C-MOVE 进行中，等待 C-STORE 回传
STATE_COMPLETED = "COMPLETED"    # 清单收齐且 C-MOVE 成功，可下载
STATE_FAILED = "FAILED"          # 终态失败（FIND/MOVE/保存/重启中断/缺件）

TERMINAL_STATES = {STATE_COMPLETED, STATE_FAILED}

# ---- 去标识包状态（同批并发生成只允许一个发布者）----
DEID_RUNNING = "RUNNING"         # 已领取生成权，正在读原包/改写/写盘
DEID_COMPLETED = "COMPLETED"     # 新包已原子发布，可下载
DEID_FAILED = "FAILED"           # 失败（含批外引用/写盘失败/重启中断），可重试

# 进程级单例（main 启动时注入），供 storage 等模块反向访问
_DB_SINGLETON: "Database | None" = None


def set_db(db: "Database") -> None:
    global _DB_SINGLETON
    _DB_SINGLETON = db


def get_db() -> "Database":
    if _DB_SINGLETON is None:
        raise RuntimeError("数据库尚未初始化")
    return _DB_SINGLETON


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(path), check_same_thread=False, timeout=30.0, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=FULL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._create_schema()

    # ------------------------------------------------------------------ schema
    def _create_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS batches (
                    id                  TEXT PRIMARY KEY,
                    study_instance_uid  TEXT NOT NULL,
                    state               TEXT NOT NULL,
                    expected_count      INTEGER NOT NULL DEFAULT 0,
                    received_count      INTEGER NOT NULL DEFAULT 0,
                    failure_reason      TEXT,
                    zip_path            TEXT,
                    fault_inject        TEXT NOT NULL DEFAULT '',
                    created_at          TEXT NOT NULL,
                    updated_at          TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_batches_study
                    ON batches(study_instance_uid);

                CREATE TABLE IF NOT EXISTS instances (
                    batch_id            TEXT NOT NULL
                        REFERENCES batches(id) ON DELETE CASCADE,
                    sop_instance_uid    TEXT NOT NULL,
                    sop_class_uid       TEXT NOT NULL,
                    transfer_syntax     TEXT NOT NULL,
                    file_path           TEXT NOT NULL,
                    received_at         TEXT NOT NULL,
                    PRIMARY KEY (batch_id, sop_instance_uid)
                );

                CREATE TABLE IF NOT EXISTS events (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id    TEXT,
                    kind        TEXT NOT NULL,
                    message     TEXT NOT NULL,
                    created_at  TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_events_batch
                    ON events(batch_id, id);

                -- 去标识包：每批最多一份。RUNNING 期间其他请求不得并发写盘；
                -- COMPLETED 后 zip_path 指向原子发布的新包；FAILED 允许重试。
                CREATE TABLE IF NOT EXISTS deid_packages (
                    batch_id      TEXT PRIMARY KEY
                        REFERENCES batches(id) ON DELETE CASCADE,
                    state         TEXT NOT NULL,
                    zip_path      TEXT,
                    attempt       INTEGER NOT NULL DEFAULT 0,
                    error         TEXT,
                    study_uid     TEXT,
                    patient_id    TEXT,
                    created_at    TEXT NOT NULL,
                    updated_at    TEXT NOT NULL
                );

                -- 旧 SOP -> 新 SOP 的稳定映射（用于重试一致性校验与清单）
                CREATE TABLE IF NOT EXISTS deid_instances (
                    batch_id           TEXT NOT NULL
                        REFERENCES batches(id) ON DELETE CASCADE,
                    original_sop_uid   TEXT NOT NULL,
                    new_sop_uid        TEXT NOT NULL,
                    file_name          TEXT NOT NULL,
                    PRIMARY KEY (batch_id, original_sop_uid)
                );
                """
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ events
    def add_event(self, kind: str, message: str, batch_id: str | None = None) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO events(batch_id, kind, message, created_at) "
                "VALUES (?, ?, ?, ?)",
                (batch_id, kind, message, _now()),
            )

    def list_events(self, batch_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT kind, message, created_at FROM events "
                "WHERE batch_id = ? ORDER BY id",
                (batch_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ batches
    def create_batch(
        self, batch_id: str, study_instance_uid: str, fault_inject: str
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO batches(id, study_instance_uid, state, fault_inject, "
                "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (batch_id, study_instance_uid, STATE_PENDING, fault_inject,
                 _now(), _now()),
            )
        self.add_event("BATCH_CREATED", f"批创建，Study={study_instance_uid}", batch_id)

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM batches WHERE id = ?", (batch_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_batches(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM batches ORDER BY rowid DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def has_active_batch_for_study(self, study_instance_uid: str) -> bool:
        """同一 Study 已有未终态批时拒绝重复提交。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM batches WHERE study_instance_uid = ? "
                "AND state NOT IN (?, ?) LIMIT 1",
                (study_instance_uid, STATE_COMPLETED, STATE_FAILED),
            ).fetchone()
        return row is not None

    def transition_state(
        self,
        batch_id: str,
        expected: Iterable[str],
        new_state: str,
        *,
        failure_reason: str | None = None,
        expected_count: int | None = None,
        zip_path: str | None = None,
    ) -> bool:
        """仅当当前状态属于 ``expected`` 时迁移，返回是否发生了迁移。

        终态（COMPLETED / FAILED）不可再被改写——这是“迟到回传不能恢复
        失败批”的数据库级保证。
        """
        expected = set(expected)
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT state FROM batches WHERE id = ?", (batch_id,)
            ).fetchone()
            if row is None or row["state"] not in expected:
                return False
            # 数据库级终态保护：COMPLETED / FAILED 一旦落定不可迁出，
            # 任何迟到事件都无法“复活”一个失败批。
            if row["state"] in TERMINAL_STATES and new_state != row["state"]:
                return False
            self._conn.execute(
                "UPDATE batches SET state = ?, failure_reason = COALESCE(?, failure_reason), "
                "zip_path = COALESCE(?, zip_path), updated_at = ? WHERE id = ?",
                (new_state, failure_reason, zip_path, _now(), batch_id),
            )
            if expected_count is not None:
                self._conn.execute(
                    "UPDATE batches SET expected_count = ? WHERE id = ?",
                    (expected_count, batch_id),
                )
        return True

    def fail_batch(self, batch_id: str, reason: str) -> bool:
        ok = self.transition_state(
            batch_id,
            [STATE_PENDING, STATE_FINDING, STATE_MOVING],
            STATE_FAILED,
            failure_reason=reason,
        )
        if ok:
            self.add_event("BATCH_FAILED", reason, batch_id)
        return ok

    def complete_batch(self, batch_id: str, zip_path: str) -> bool:
        ok = self.transition_state(
            batch_id, [STATE_MOVING], STATE_COMPLETED, zip_path=zip_path
        )
        if ok:
            self.add_event("BATCH_COMPLETED", "清单收齐，ZIP 已生成", batch_id)
        return ok

    # ------------------------------------------------------------------ manifest
    def set_manifest(
        self, batch_id: str, items: list[tuple[str, str, str]]
    ) -> None:
        """登记 C-FIND 固定下来的 SOP 清单。

        ``items`` 元素为 ``(sop_instance_uid, sop_class_uid, transfer_syntax)``。
        同一批只允许固定一次。
        """
        with self._lock, self._conn:
            exists = self._conn.execute(
                "SELECT 1 FROM instances WHERE batch_id = ? LIMIT 1", (batch_id,)
            ).fetchone()
            if exists:
                raise RuntimeError("manifest already fixed")
            self._conn.executemany(
                "INSERT INTO instances(batch_id, sop_instance_uid, sop_class_uid, "
                "transfer_syntax, file_path, received_at) VALUES (?, ?, ?, ?, '', '')",
                [(batch_id, sop, cls, ts) for sop, cls, ts in items],
            )
            self._conn.execute(
                "UPDATE batches SET expected_count = ?, updated_at = ? WHERE id = ?",
                (len(items), _now(), batch_id),
            )

    def manifest_uids(self, batch_id: str) -> set[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT sop_instance_uid FROM instances WHERE batch_id = ?",
                (batch_id,),
            ).fetchall()
        return {r["sop_instance_uid"] for r in rows}

    def is_manifest_uid(self, batch_id: str, sop_uid: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM instances WHERE batch_id = ? AND sop_instance_uid = ?",
                (batch_id, sop_uid),
            ).fetchone()
        return row is not None

    def is_already_received(self, batch_id: str, sop_uid: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM instances WHERE batch_id = ? AND sop_instance_uid = ? "
                "AND file_path != ''",
                (batch_id, sop_uid),
            ).fetchone()
        return row is not None

    def register_instance(
        self, batch_id: str, sop_uid: str, file_path: str, transfer_syntax: str
    ) -> bool:
        """登记一个已落盘实例。返回 False 表示重复投送（不重复计数）。"""
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT file_path FROM instances WHERE batch_id = ? AND sop_instance_uid = ?",
                (batch_id, sop_uid),
            ).fetchone()
            if row is None:
                return False  # 不在清单内，调用方应拒绝
            if row["file_path"] != "":
                return False  # 重复实例
            self._conn.execute(
                "UPDATE instances SET file_path = ?, transfer_syntax = ?, "
                "received_at = ? WHERE batch_id = ? AND sop_instance_uid = ?",
                (file_path, transfer_syntax, _now(), batch_id, sop_uid),
            )
            self._conn.execute(
                "UPDATE batches SET received_count = received_count + 1, updated_at = ? "
                "WHERE id = ?",
                (_now(), batch_id),
            )
        return True

    def received_progress(self, batch_id: str) -> tuple[int, int]:
        with self._lock:
            row = self._conn.execute(
                "SELECT expected_count, received_count FROM batches WHERE id = ?",
                (batch_id,),
            ).fetchone()
        if row is None:
            return (0, 0)
        return row["expected_count"], row["received_count"]

    def missing_uids(self, batch_id: str) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT sop_instance_uid FROM instances WHERE batch_id = ? AND file_path = ''",
                (batch_id,),
            ).fetchall()
        return [r["sop_instance_uid"] for r in rows]

    def received_files(self, batch_id: str) -> list[tuple[str, str]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT sop_instance_uid, file_path FROM instances "
                "WHERE batch_id = ? AND file_path != '' ORDER BY sop_instance_uid",
                (batch_id,),
            ).fetchall()
        return [(r["sop_instance_uid"], r["file_path"]) for r in rows]

    def find_batch_for_study(self, study_uid: str) -> dict[str, Any] | None:
        """按 StudyInstanceUID 路由 C-STORE 回传到对应批。

        用 rowid 而不是时间戳排序：同秒内创建多个批时顺序仍确定，
        永远命中最新的批。
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM batches WHERE study_instance_uid = ? "
                "ORDER BY rowid DESC LIMIT 1",
                (study_uid,),
            ).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------ recovery
    def non_terminal_batches(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM batches WHERE state NOT IN (?, ?)",
                (STATE_COMPLETED, STATE_FAILED),
            ).fetchall()
        return [dict(r) for r in rows]

    def all_completed_batches(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM batches WHERE state = ?", (STATE_COMPLETED,)
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------ deid packages
    def get_deid(self, batch_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM deid_packages WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        return dict(row) if row else None

    def claim_deid(self, batch_id: str) -> bool:
        """原子领取去标识生成权。

        只有不存在记录、或上次为 FAILED 时才允许领取；领取即写入 RUNNING，
        从而同批并发生成只有一个发布者。COMPLETED 与 RUNNING 均拒绝领取。

        用 ``INSERT ... ON CONFLICT DO UPDATE ... WHERE state='FAILED'`` 让
        “检查 + 写入”在单条 SQL 内完成：即使未来以多个 worker 进程共享
        同一个 SQLite（WAL）运行，也只有一个进程能领取成功。
        """
        now = _now()
        with self._lock, self._conn:
            before = self._conn.total_changes
            self._conn.execute(
                "INSERT INTO deid_packages(batch_id, state, attempt, "
                "created_at, updated_at) VALUES (?, ?, 1, ?, ?) "
                "ON CONFLICT(batch_id) DO UPDATE SET "
                "  state = excluded.state, "
                "  error = NULL, "
                "  attempt = deid_packages.attempt + 1, "
                "  updated_at = excluded.updated_at "
                "WHERE deid_packages.state = ?",
                (batch_id, DEID_RUNNING, now, now, DEID_FAILED),
            )
            return self._conn.total_changes > before

    def complete_deid(
        self,
        batch_id: str,
        zip_path: str,
        *,
        study_uid: str,
        patient_id: str,
        instances: list[tuple[str, str, str]],
    ) -> bool:
        """登记去标识结果：SOP 映射与新包路径在同一事务内落定。"""
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT state FROM deid_packages WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if row is None or row["state"] != DEID_RUNNING:
                return False
            self._conn.executemany(
                "INSERT OR REPLACE INTO deid_instances(batch_id, original_sop_uid, "
                "new_sop_uid, file_name) VALUES (?, ?, ?, ?)",
                [(batch_id, old, new, name) for old, new, name in instances],
            )
            self._conn.execute(
                "UPDATE deid_packages SET state = ?, zip_path = ?, study_uid = ?, "
                "patient_id = ?, error = NULL, updated_at = ? WHERE batch_id = ?",
                (DEID_COMPLETED, zip_path, study_uid, patient_id, _now(), batch_id),
            )
        return True

    def fail_deid(self, batch_id: str, reason: str) -> bool:
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT state FROM deid_packages WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if row is None or row["state"] != DEID_RUNNING:
                return False
            self._conn.execute(
                "UPDATE deid_packages SET state = ?, error = ?, updated_at = ? "
                "WHERE batch_id = ?",
                (DEID_FAILED, reason, _now(), batch_id),
            )
        return True

    def deid_instances(self, batch_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT original_sop_uid, new_sop_uid, file_name "
                "FROM deid_instances WHERE batch_id = ? ORDER BY original_sop_uid",
                (batch_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def all_deid_packages(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM deid_packages ORDER BY rowid"
            ).fetchall()
        return [dict(r) for r in rows]

    def recover_deid_running(self, reason: str) -> list[str]:
        """启动恢复：把上次进程遗留的 RUNNING 一律置 FAILED（可重试）。"""
        with self._lock, self._conn:
            rows = self._conn.execute(
                "SELECT batch_id FROM deid_packages WHERE state = ?",
                (DEID_RUNNING,),
            ).fetchall()
            ids = [r["batch_id"] for r in rows]
            if ids:
                self._conn.executemany(
                    "UPDATE deid_packages SET state = ?, error = ?, updated_at = ? "
                    "WHERE batch_id = ?",
                    [(DEID_FAILED, reason, _now(), bid) for bid in ids],
                )
        return ids

    def invalidate_deid_package(self, batch_id: str, reason: str) -> bool:
        """已发布包文件丢失时置 FAILED（允许重新生成）。"""
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT state FROM deid_packages WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if row is None or row["state"] != DEID_COMPLETED:
                return False
            self._conn.execute(
                "UPDATE deid_packages SET state = ?, zip_path = NULL, error = ?, "
                "updated_at = ? WHERE batch_id = ?",
                (DEID_FAILED, reason, _now(), batch_id),
            )
        return True
