"""文件目录 (catalog): 每个文件的实际格式与校验值逐文件登记。

目录记录的是"期望值", 磁盘是"实际值"; 审计/消费时两者比对。
绝不用全局格式名推断单个文件的格式 —— 混合格式期间, 只有这里的登记算数。
"""
from __future__ import annotations

import json
import os
import threading

from . import common

_lock = threading.RLock()

DEFAULT = {
    "config": {"write_format": "gzip"},  # 只影响"新写入", 不回溯解释存量文件
    "partitions": {},  # partition -> {parts, rows, updated_at}
    "files": {},       # file_id -> 登记项
    "consumers": {},   # name -> {capabilities, checkpoint, last_run}
}


def load() -> dict:
    with _lock:
        if common.CATALOG_PATH.exists():
            return json.loads(common.CATALOG_PATH.read_text(encoding="utf-8"))
        return json.loads(json.dumps(DEFAULT))


def save(cat: dict) -> None:
    with _lock:
        common.ensure_dirs()
        tmp = common.CATALOG_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(cat, indent=2, sort_keys=True, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, common.CATALOG_PATH)


def transact(fn):
    """load -> fn(cat) -> save, 持锁保证网页并发请求不撕碎目录。fn 抛异常则不落盘。"""
    with _lock:
        cat = load()
        result = fn(cat)
        save(cat)
        return result


def active_files(cat: dict, partition: str | None = None) -> list[dict]:
    files = [e for e in cat["files"].values() if e.get("state") == "active"]
    if partition is not None:
        files = [e for e in files if e["partition"] == partition]
    return sorted(files, key=lambda e: (e["partition"], e["part"]))
