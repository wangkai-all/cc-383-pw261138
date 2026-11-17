"""共享基础：路径、规范编码、哈希。

数据流向: raw/<分区>.jsonl (上游源, 视为对象存储里的原始暂存)
        -> warehouse/dt=<分区>/part-NNN.jsonl.<ext> (仓内压缩文件)
        -> catalog.json (逐文件登记: 格式 / 文件校验值 / 内容校验值 / 行数)
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("WAREHOUSE_DATA", REPO_ROOT / "data"))

RAW_DIR = DATA_DIR / "raw"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
OUT_DIR = DATA_DIR / "out"
CATALOG_PATH = DATA_DIR / "catalog.json"


def ensure_dirs() -> None:
    for d in (RAW_DIR, WAREHOUSE_DIR, OUT_DIR):
        d.mkdir(parents=True, exist_ok=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---- 规范 payload: JSONL, 键排序, UTF-8, \n 结尾 -------------------------
# 内容校验值针对 *解压后* 的规范字节计算, 因此与压缩格式无关,
# 换格式重编码后内容校验值必须保持不变 —— 这是数据核对的核心不变量。

def canonical_line(row: dict) -> bytes:
    return (json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")


def encode_rows(rows: list[dict]) -> bytes:
    return b"".join(canonical_line(r) for r in rows)


def decode_payload(payload: bytes) -> list[dict]:
    return [json.loads(line) for line in payload.decode("utf-8").splitlines() if line.strip()]


def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def chain_hash(prev_hex: str, content_hex: str) -> str:
    """可续接的读取链哈希: H(prev || content). 断点续读时无需重放历史。"""
    return hashlib.sha256((prev_hex + content_hex).encode("ascii")).hexdigest()


# ---- 文件标识与路径 -------------------------------------------------------

def file_id(partition: str, part: int) -> str:
    return f"{partition}/part-{part:03d}"


def warehouse_path(partition: str, part: int, ext: str) -> Path:
    return WAREHOUSE_DIR / f"dt={partition}" / f"part-{part:03d}.jsonl.{ext}"


def split_rows(rows: list[dict], n: int) -> list[list[dict]]:
    """确定性连续切分, 恢复单个 part 时可按同样规则重放。"""
    n = max(1, min(n, len(rows)))
    size = -(-len(rows) // n)
    return [rows[i * size:(i + 1) * size] for i in range(n)]


def atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)
