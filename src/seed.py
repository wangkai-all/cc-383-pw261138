"""演示数据: 生成 raw 分区, 老分区按 gzip 入仓, 切换写入格式后新分区按 xz 入仓,
并登记两个能力不同的消费端 (consumer-a 只会 gzip, consumer-b 会 gzip+xz)。

用法: python -m src.seed [--reset]
"""
from __future__ import annotations

import argparse
import random
import shutil

from . import catalog, codecs, common, ingest

GZIP_PARTITIONS = ["2026-11-08", "2026-11-09", "2026-11-10", "2026-11-11", "2026-11-12"]
XZ_PARTITIONS = ["2026-11-13", "2026-11-14", "2026-11-15", "2026-11-16"]


def _gen_raw(partition: str) -> int:
    rng = random.Random(f"seed:{partition}")
    n = rng.randint(120, 400)
    rows = []
    base_ts = 1763000000
    for i in range(n):
        rows.append({
            "id": f"{partition}-{i:06d}",
            "ts": base_ts + rng.randint(0, 86400),
            "metric": rng.choice(["cpu", "mem", "io", "net"]),
            "node": f"host-{rng.randint(1, 12):02d}",
            "value": round(rng.uniform(0, 100), 3),
        })
    common.ensure_dirs()
    common.atomic_write(common.RAW_DIR / f"{partition}.jsonl", common.encode_rows(rows))
    return n


def run(reset: bool = False) -> dict:
    if reset and common.DATA_DIR.exists():
        shutil.rmtree(common.DATA_DIR)
    common.ensure_dirs()
    summary = {"raw": {}, "gzip": [], "xz": []}
    for p in GZIP_PARTITIONS + XZ_PARTITIONS:
        summary["raw"][p] = _gen_raw(p)
    for p in GZIP_PARTITIONS:  # 历史分区: 旧格式入仓
        ingest.ingest_partition(p, parts=2, fmt=codecs.OLD_FORMAT, replace=True)
        summary["gzip"].append(p)
    def switch(cat: dict) -> None:  # 切换全局写入格式 —— 只影响新写入
        cat["config"]["write_format"] = codecs.NEW_FORMAT
    catalog.transact(switch)
    for p in XZ_PARTITIONS:  # 新写入: 新格式
        ingest.ingest_partition(p, parts=2, fmt=codecs.NEW_FORMAT, replace=True)
        summary["xz"].append(p)
    def declare(cat: dict) -> None:
        cat["consumers"]["consumer-a"] = {"capabilities": [codecs.OLD_FORMAT]}  # 旧消费端
        cat["consumers"]["consumer-b"] = {"capabilities": [codecs.OLD_FORMAT, codecs.NEW_FORMAT]}
    catalog.transact(declare)
    summary["consumers"] = {"consumer-a": ["gzip"], "consumer-b": ["gzip", "xz"]}
    return summary


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="清空数据目录后重建")
    args = ap.parse_args(argv)
    s = run(reset=args.reset)
    print(f"raw 分区: {list(s['raw'])}")
    print(f"gzip 入仓: {s['gzip']}")
    print(f"xz 入仓:   {s['xz']}")
    print(f"消费端:    {s['consumers']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
