"""运维操作: 分区格式转换、损坏注入、恢复、全量核对、旧解码器停用评估。"""
from __future__ import annotations

from . import catalog, codecs, common


class OpsError(Exception):
    pass


def set_write_format(fmt: str) -> dict:
    if fmt not in codecs.FORMATS:
        raise OpsError(f"未知格式: {fmt}")

    def work(cat: dict) -> dict:
        old = cat["config"]["write_format"]
        cat["config"]["write_format"] = fmt
        return {"old": old, "new": fmt,
                "note": "只影响之后的新写入; 存量文件的格式以目录逐文件登记为准, 不会被改名。"}
    return catalog.transact(work)


def convert_partition(partition: str, target: str) -> dict:
    """把指定分区的所有 active 文件转码到 target 格式。

    流程: 核旧文件校验值 -> 解压 -> 重编码 -> 回放核对(内容校验值必须不变) -> 原子替换 -> 更新登记。
    内容校验值与格式无关, 转换前后必须逐字节一致, 否则中止。
    """
    if target not in codecs.FORMATS:
        raise OpsError(f"未知格式: {target}")

    def work(cat: dict) -> dict:
        entries = catalog.active_files(cat, partition)
        if not entries:
            raise OpsError(f"分区 {partition} 没有 active 文件")
        converted, skipped = [], []
        for e in entries:
            fid = e["id"]
            if e["format"] == target:
                skipped.append(fid)
                continue
            src = common.DATA_DIR / e["path"]
            if not src.exists():
                raise OpsError(f"{fid} 文件缺失, 请先恢复再转换")
            blob = src.read_bytes()
            if common.sha256_hex(blob) != e["file_sha256"]:
                raise OpsError(f"{fid} 校验值不符(已损坏), 请先恢复再转换")
            payload = codecs.FORMATS[e["format"]]["decompress"](blob)
            if common.sha256_hex(payload) != e["content_sha256"]:
                raise OpsError(f"{fid} 解压后内容与登记不符, 中止转换")
            new_blob = codecs.FORMATS[target]["compress"](payload)
            # 回放核对: 新文件解压后的内容校验值必须与登记逐字节一致
            roundtrip = codecs.FORMATS[target]["decompress"](new_blob)
            if common.sha256_hex(roundtrip) != e["content_sha256"]:
                raise OpsError(f"{fid} 转码回放核对失败, 中止转换")
            new_path = common.warehouse_path(partition, e["part"], codecs.FORMATS[target]["ext"])
            common.atomic_write(new_path, new_blob)
            src.unlink()
            e.update(format=target,
                     path=str(new_path.relative_to(common.DATA_DIR)),
                     file_sha256=common.sha256_hex(new_blob),
                     bytes=len(new_blob),
                     updated_at=common.now_iso())
            converted.append(fid)
        return {"partition": partition, "target": target, "converted": converted,
                "skipped(已是目标格式)": skipped}

    return catalog.transact(work)


def corrupt_file(fid: str) -> dict:
    """故障注入: 翻转文件中间的两个字节。目录不动 —— 损坏由审计/消费端通过校验值发现。"""
    cat = catalog.load()
    e = cat["files"].get(fid)
    if not e:
        raise OpsError(f"未知文件: {fid}")
    path = common.DATA_DIR / e["path"]
    if not path.exists():
        raise OpsError(f"{fid} 已缺失, 无需再损坏")
    data = bytearray(path.read_bytes())
    if common.sha256_hex(bytes(data)) != e["file_sha256"]:
        raise OpsError(f"{fid} 当前已与登记不符, 无需再损坏")
    for pos in (len(data) // 3, 2 * len(data) // 3):
        data[pos] ^= 0xFF
    path.write_bytes(bytes(data))
    return {"file": fid, "flipped": [len(data) // 3, 2 * len(data) // 3],
            "note": "目录登记未动; 运行审计或消费端即可发现校验值不符"}


def recover_file(fid: str) -> dict:
    """从 raw 源按确定性切分重放出该 part, 核对内容校验值后原子替换并更新登记。"""
    def work(cat: dict) -> dict:
        e = cat["files"].get(fid)
        if not e:
            raise OpsError(f"未知文件: {fid}")
        path = common.DATA_DIR / e["path"]
        if path.exists() and common.file_sha256(path) == e["file_sha256"]:
            return {"file": fid, "recovered": False, "note": "文件与登记一致, 无需恢复"}
        raw = common.RAW_DIR / f"{e['partition']}.jsonl"
        if not raw.exists():
            raise OpsError(f"缺少 raw 源 {raw}, 无法安全恢复 {fid}")
        rows = common.decode_payload(raw.read_bytes())
        parts = cat["partitions"][e["partition"]]["parts"]
        chunk = common.split_rows(rows, parts)[e["part"]]
        payload = common.encode_rows(chunk)
        if common.sha256_hex(payload) != e["content_sha256"]:
            raise OpsError(f"raw 源重放的内容校验值与目录登记不符, 拒绝恢复 {fid} (上游数据可能已变)")
        blob = codecs.FORMATS[e["format"]]["compress"](payload)
        common.atomic_write(path, blob)
        e.update(file_sha256=common.sha256_hex(blob), bytes=len(blob), updated_at=common.now_iso())
        return {"file": fid, "recovered": True, "format": e["format"], "rows": len(chunk)}

    return catalog.transact(work)


def audit() -> dict:
    """全量数据核对: 目录(期望) vs 磁盘(实际) vs raw 源(上游)。任何一项不符 ok=False。"""
    cat = catalog.load()
    report = {"ok": True, "files": [], "partitions": [], "checked_at": common.now_iso()}
    for e in catalog.active_files(cat):
        fid = e["id"]
        entry = {"file": fid, "format": e["format"], "ok": True, "problems": []}
        path = common.DATA_DIR / e["path"]
        if not path.exists():
            entry["ok"] = False
            entry["problems"].append("文件缺失")
        else:
            actual = common.file_sha256(path)
            if actual != e["file_sha256"]:
                entry["ok"] = False
                entry["problems"].append(f"文件校验值不符 (实际 {actual[:16]}…)")
            else:
                try:
                    payload = codecs.FORMATS[e["format"]]["decompress"](path.read_bytes())
                    if common.sha256_hex(payload) != e["content_sha256"]:
                        entry["ok"] = False
                        entry["problems"].append("内容校验值不符")
                    if len(common.decode_payload(payload)) != e["rows"]:
                        entry["ok"] = False
                        entry["problems"].append("行数不符")
                except Exception as exc:
                    entry["ok"] = False
                    entry["problems"].append(f"解压失败: {exc}")
        report["files"].append(entry)
        report["ok"] = report["ok"] and entry["ok"]
    for partition, meta in sorted(cat["partitions"].items()):
        pentry = {"partition": partition, "ok": True, "problems": []}
        files = catalog.active_files(cat, partition)
        registered = sum(e["rows"] for e in files)
        if registered != meta["rows"]:
            pentry["ok"] = False
            pentry["problems"].append(f"登记行数 {registered} != 分区行数 {meta['rows']}")
        raw = common.RAW_DIR / f"{partition}.jsonl"
        if raw.exists():
            raw_rows = common.decode_payload(raw.read_bytes())
            if len(raw_rows) != meta["rows"]:
                pentry["ok"] = False
                pentry["problems"].append(f"raw 源 {len(raw_rows)} 行 != 登记 {meta['rows']} 行")
            else:
                chunks = common.split_rows(raw_rows, meta["parts"])
                for e in files:
                    if common.sha256_hex(common.encode_rows(chunks[e["part"]])) != e["content_sha256"]:
                        pentry["ok"] = False
                        pentry["problems"].append(f"{e['id']} 与 raw 源重放内容不符")
        else:
            pentry["problems"].append("raw 源缺失, 跳过上游核对(仅做了目录 vs 磁盘核对)")
        report["partitions"].append(pentry)
        report["ok"] = report["ok"] and pentry["ok"]
    return report


def retire_check(old: str = codecs.OLD_FORMAT, new: str = codecs.NEW_FORMAT) -> dict:
    """旧解码器何时能停用 —— 三个条件同时满足才行:
    1. 目录中已没有任何 active 文件使用旧格式 (全部转换或已下线);
    2. 所有已登记消费端都声明了新格式能力 (否则转换后它们读不了);
    3. 此刻全量核对通过 (没有待处理的损坏)。
    """
    cat = catalog.load()
    old_files = [e["id"] for e in catalog.active_files(cat) if e["format"] == old]
    consumers = cat["consumers"]
    lacking = {n: c.get("capabilities", []) for n, c in consumers.items() if new not in c.get("capabilities", [])}
    conditions = {
        "no_active_old_format_files": {"pass": not old_files, "remaining": old_files},
        "all_consumers_support_new_format": {"pass": bool(consumers) and not lacking, "lacking": lacking},
        "audit_clean": {"pass": audit()["ok"]},
    }
    return {"old_format": old, "new_format": new,
            "can_retire_old_decoder": all(c["pass"] for c in conditions.values()),
            "conditions": conditions}
