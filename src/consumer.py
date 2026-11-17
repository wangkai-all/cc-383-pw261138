"""消费端: 独立进程运行, 声明解码能力, 按序读取并全程核对。

铁律 —— 旧消费者不能静默漏行:
  * 遇到目录登记格式不在自己能力表里的文件 -> 立即停下, 显式报错, 退出码 3。
    不跳过该文件继续读后面的 (跳过=静默漏行), 不提交该文件的任何行。
  * 文件校验值不符 / 解压失败 / 内容校验值或行数不符 -> 退出码 4, 同样不放行任何行。
  * 每个文件完整核对通过后才提交断点; 中途失败时断点停在最后一个核对通过的文件,
    升级能力或转换分区后从断点续跑, 不重不漏。

退出码: 0=读完全部  3=遇到不支持的格式  4=数据损坏/核对失败  2=参数错误
"""
from __future__ import annotations

import argparse
import json
import sys

from . import catalog, codecs, common

EXIT_OK = 0
EXIT_UNSUPPORTED = 3
EXIT_CORRUPT = 4

ZERO_HASH = "0" * 64


class ConsumerStop(Exception):
    def __init__(self, exit_code: int, message: str):
        super().__init__(message)
        self.exit_code = exit_code
        self.message = message


def _record_run(name: str, status: str, detail: str) -> None:
    def work(cat: dict) -> None:
        cat["consumers"].setdefault(name, {})["last_run"] = {
            "status": status, "detail": detail, "at": common.now_iso(),
        }
    catalog.transact(work)


def _commit_checkpoint(name: str, entry: dict, read_hash: str, rows: int) -> None:
    def work(cat: dict) -> None:
        cat["consumers"].setdefault(name, {})["checkpoint"] = {
            "last_key": [entry["partition"], entry["part"]],
            "last_file": entry["id"],
            "read_hash": read_hash,
            "rows": rows,
            "at": common.now_iso(),
        }
    catalog.transact(work)


def run_consumer(name: str, capabilities: list[str] | None = None, from_scratch: bool = False,
                 log=print) -> int:
    cat = catalog.load()
    reg = cat["consumers"].get(name, {})
    caps = list(capabilities) if capabilities else reg.get("capabilities") or ["gzip"]
    unknown = [c for c in caps if c not in codecs.FORMATS]
    if unknown:
        print(f"ERROR 未知能力声明: {unknown}", file=sys.stderr)
        return 2

    def declare(c: dict) -> None:
        c["consumers"].setdefault(name, {})["capabilities"] = caps
    catalog.transact(declare)

    files = catalog.active_files(catalog.load())
    ckpt = None if from_scratch else reg.get("checkpoint")
    if ckpt:
        last = tuple(ckpt["last_key"])
        files = [e for e in files if (e["partition"], e["part"]) > last]
        read_hash, total_rows = ckpt["read_hash"], ckpt["rows"]
        log(f"RESUME {name} 从断点 {ckpt['last_file']} 之后续跑 (已读 {total_rows} 行)")
    else:
        read_hash, total_rows = ZERO_HASH, 0

    manifest = []
    read_files = 0
    try:
        for e in files:
            fid, fmt = e["id"], e["format"]
            # 1) 能力检查: 不支持就停下报错, 绝不跳过
            if fmt not in caps:
                raise ConsumerStop(EXIT_UNSUPPORTED,
                                   f"UNSUPPORTED FORMAT 文件 {fid} 登记格式为 '{fmt}', 消费端 '{name}' "
                                   f"只声明了 {caps}。已在该文件前停下: 未发出它的任何行, 也未跳过它。"
                                   f"处理办法: 升级该消费端解码能力, 或把分区 {e['partition']} 转换为支持的格式后重跑。")
            path = common.DATA_DIR / e["path"]
            # 2) 先核文件校验值, 通过前不解压、不放行任何行
            if not path.exists():
                raise ConsumerStop(EXIT_CORRUPT, f"MISSING 文件 {fid} 在磁盘上不存在 ({path})")
            actual = common.file_sha256(path)
            if actual != e["file_sha256"]:
                raise ConsumerStop(EXIT_CORRUPT,
                                   f"CHECKSUM MISMATCH 文件 {fid} 实际 sha256={actual[:16]}… "
                                   f"与目录登记 {e['file_sha256'][:16]}… 不符, 疑似损坏, 拒绝读取。")
            # 3) 解压并做内容核对 (行数 + 内容校验值)
            try:
                payload = codecs.FORMATS[fmt]["decompress"](path.read_bytes())
            except Exception as exc:
                raise ConsumerStop(EXIT_CORRUPT, f"DECODE FAIL 文件 {fid} 解压失败: {exc}") from exc
            if common.sha256_hex(payload) != e["content_sha256"]:
                raise ConsumerStop(EXIT_CORRUPT, f"CONTENT MISMATCH 文件 {fid} 解压后内容校验值与目录登记不符")
            rows = common.decode_payload(payload)
            if len(rows) != e["rows"]:
                raise ConsumerStop(EXIT_CORRUPT,
                                   f"ROW COUNT MISMATCH 文件 {fid} 实际 {len(rows)} 行, 目录登记 {e['rows']} 行")
            # 4) 全部核对通过: 才算"读到", 提交断点
            read_hash = common.chain_hash(read_hash, e["content_sha256"])
            total_rows += len(rows)
            read_files += 1
            manifest.append({"file": fid, "format": fmt, "rows": len(rows), "content_sha256": e["content_sha256"]})
            _commit_checkpoint(name, e, read_hash, total_rows)
            log(f"OK {fid} format={fmt} rows={len(rows)}")
    except ConsumerStop as stop:
        _record_run(name, "unsupported" if stop.exit_code == EXIT_UNSUPPORTED else "corrupt", stop.message)
        print(f"ERROR {stop.message}", file=sys.stderr)
        print(f"STOPPED consumer={name} 已提交断点行数={total_rows} 本次完整读出文件数={read_files}", file=sys.stderr)
        return stop.exit_code

    common.ensure_dirs()
    (common.OUT_DIR / f"{name}.manifest.json").write_text(json.dumps({
        "consumer": name, "capabilities": caps, "files": manifest,
        "total_rows": total_rows, "read_hash": read_hash, "completed_at": common.now_iso(),
    }, indent=2, ensure_ascii=False), encoding="utf-8")
    _record_run(name, "complete", f"读完全部, 累计 {total_rows} 行, read_hash={read_hash[:16]}…")
    log(f"COMPLETE consumer={name} files={read_files} total_rows={total_rows} read_hash={read_hash[:16]}…")
    return EXIT_OK


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="消费端")
    ap.add_argument("--name", required=True, help="消费端标识, 如 consumer-a")
    ap.add_argument("--cap", help="声明解码能力, 逗号分隔, 如 gzip,xz; 缺省沿用目录中已登记的能力")
    ap.add_argument("--from-scratch", action="store_true", help="忽略断点从头读")
    args = ap.parse_args(argv)
    caps = [c.strip() for c in args.cap.split(",") if c.strip()] if args.cap else None
    return run_consumer(args.name, caps, args.from_scratch)


if __name__ == "__main__":
    raise SystemExit(main())
