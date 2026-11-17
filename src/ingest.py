"""入仓转换器: 读 raw 暂存区 -> 按当前写入格式压缩 -> 逐文件登记目录。

用法:
  python -m src.ingest --partition 2026-11-17 [--parts 2] [--format xz] [--replace]
  python -m src.ingest --all          # 入仓所有尚未登记的 raw 分区
"""
from __future__ import annotations

import argparse
import sys

from . import catalog, codecs, common


class IngestError(Exception):
    pass


def ingest_partition(partition: str, parts: int = 1, fmt: str | None = None, replace: bool = False) -> dict:
    raw = common.RAW_DIR / f"{partition}.jsonl"
    if not raw.exists():
        raise IngestError(f"缺少 raw 源文件: {raw}")
    rows = common.decode_payload(raw.read_bytes())
    if not rows:
        raise IngestError(f"分区 {partition} 的 raw 源为空, 拒绝入仓")

    def work(cat: dict) -> dict:
        write_fmt = fmt or cat["config"]["write_format"]
        if write_fmt not in codecs.FORMATS:
            raise IngestError(f"未知格式: {write_fmt}")
        existing = catalog.active_files(cat, partition)
        if existing and not replace:
            raise IngestError(f"分区 {partition} 已入仓 ({len(existing)} 个文件), 如需重做请加 --replace")
        for e in existing:  # 重做: 先清掉旧文件与登记
            cat["files"].pop(e["id"], None)
            p = common.DATA_DIR / e["path"]
            if p.exists():
                p.unlink()

        codec = codecs.FORMATS[write_fmt]
        chunks = common.split_rows(rows, parts)
        registered = []
        for i, chunk in enumerate(chunks):
            payload = common.encode_rows(chunk)
            blob = codec["compress"](payload)
            path = common.warehouse_path(partition, i, codec["ext"])
            common.atomic_write(path, blob)
            fid = common.file_id(partition, i)
            cat["files"][fid] = {
                "id": fid,
                "partition": partition,
                "part": i,
                "path": str(path.relative_to(common.DATA_DIR)),
                "format": write_fmt,
                "file_sha256": common.sha256_hex(blob),      # 压缩字节校验: 发现损坏
                "content_sha256": common.sha256_hex(payload),  # 内容校验: 跨格式核对
                "rows": len(chunk),
                "bytes": len(blob),
                "state": "active",
                "updated_at": common.now_iso(),
            }
            registered.append(fid)
        cat["partitions"][partition] = {"parts": len(chunks), "rows": len(rows), "updated_at": common.now_iso()}
        return {"partition": partition, "format": write_fmt, "files": registered, "rows": len(rows)}

    return catalog.transact(work)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="入仓转换器")
    ap.add_argument("--partition", help="入仓指定分区 (raw/<partition>.jsonl)")
    ap.add_argument("--all", action="store_true", help="入仓所有未登记的 raw 分区")
    ap.add_argument("--parts", type=int, default=1, help="每分区切分文件数")
    ap.add_argument("--format", choices=sorted(codecs.FORMATS), default=None, help="覆盖当前写入格式")
    ap.add_argument("--replace", action="store_true", help="分区已入仓时重做")
    args = ap.parse_args(argv)

    try:
        if args.all:
            done, skipped = [], []
            cat = catalog.load()
            for raw in sorted(common.RAW_DIR.glob("*.jsonl")):
                partition = raw.stem
                if catalog.active_files(cat, partition) and not args.replace:
                    skipped.append(partition)
                    continue
                done.append(ingest_partition(partition, args.parts, args.format, args.replace)["partition"])
            print(f"ingested={done} skipped(已存在)={skipped}")
            return 0
        if not args.partition:
            ap.error("需要 --partition 或 --all")
        result = ingest_partition(args.partition, args.parts, args.format, args.replace)
        print(f"OK partition={result['partition']} format={result['format']} "
              f"rows={result['rows']} files={len(result['files'])}")
        return 0
    except IngestError as e:
        print(f"ERROR {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
