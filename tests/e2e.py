"""端到端核对脚本: 真实压缩/解压/转换/损坏/恢复/停用评估全链路。

用法: python tests/e2e.py   (在独立临时数据目录中运行, 不碰演示数据)
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
FAILURES = []


def run(mod: str, *args: str, data: Path) -> subprocess.CompletedProcess:
    env = dict(os.environ, WAREHOUSE_DATA=str(data))
    return subprocess.run([sys.executable, "-m", f"src.{mod}", *args],
                          cwd=REPO, env=env, capture_output=True, text=True, timeout=120)


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"{'PASS' if cond else 'FAIL'}  {name}" + (f"  | {detail}" if detail else ""))
    if not cond:
        FAILURES.append(name)


def load_catalog(data: Path) -> dict:
    return json.loads((data / "catalog.json").read_text())


def raw_rows(data: Path, partition: str) -> int:
    return sum(1 for line in (data / "raw" / f"{partition}.jsonl").read_text().splitlines() if line.strip())


def main() -> int:
    data = Path(tempfile.mkdtemp(prefix="fmt-migration-e2e-"))
    print(f"数据目录: {data}\n--- 1. 生成演示数据 (5 个 gzip 历史分区 + 4 个 xz 新分区, 各 2 文件) ---")
    p = run("seed", "--reset", data=data)
    check("seed 成功", p.returncode == 0, p.stderr.strip())
    cat = load_catalog(data)
    fmts = [e["format"] for e in cat["files"].values()]
    check("gzip 文件 10 个", fmts.count("gzip") == 10)
    check("xz 文件 8 个", fmts.count("xz") == 8)
    total_raw = sum(raw_rows(data, part) for part in cat["partitions"])
    total_registered = sum(e["rows"] for e in cat["files"].values())
    check("登记总行数 == raw 总行数", total_registered == total_raw, f"{total_registered} == {total_raw}")

    print("\n--- 2. consumer-b (gzip+xz) 全量读取 ---")
    p = run("consumer", "--name", "consumer-b", "--from-scratch", data=data)
    check("退出码 0", p.returncode == 0, p.stderr.strip())
    manifest = json.loads((data / "out" / "consumer-b.manifest.json").read_text())
    check("读到全部行", manifest["total_rows"] == total_raw,
          f"{manifest['total_rows']} == {total_raw}")

    print("\n--- 3. consumer-a (仅 gzip) 读到一半遇到 xz: 必须停下, 不得静默漏行 ---")
    p = run("consumer", "--name", "consumer-a", "--from-scratch", data=data)
    check("退出码 3 (不支持的格式)", p.returncode == 3, p.stderr.strip().splitlines()[0] if p.stderr else "")
    check("stderr 明确报 UNSUPPORTED", "UNSUPPORTED FORMAT" in p.stderr)
    check("没有 COMPLETE (未伪装成读完全部)", "COMPLETE" not in p.stdout)
    ok_lines = [l for l in p.stdout.splitlines() if l.startswith("OK ")]
    check("只为 gzip 文件发过 OK", all("part-" in l for l in ok_lines) and len(ok_lines) == 10,
          f"OK 行数={len(ok_lines)}")
    check("未生成 consumer-a 的完成清单", not (data / "out" / "consumer-a.manifest.json").exists())
    cat = load_catalog(data)
    ckpt = cat["consumers"]["consumer-a"]["checkpoint"]
    gzip_rows = sum(raw_rows(data, part) for part in
                    ["2026-11-08", "2026-11-09", "2026-11-10", "2026-11-11", "2026-11-12"])
    check("断点停在最后一个核对通过的 gzip 文件", ckpt["last_file"] == "2026-11-12/part-001", ckpt["last_file"])
    check("断点行数 == 全部 gzip 分区行数", ckpt["rows"] == gzip_rows, f"{ckpt['rows']} == {gzip_rows}")
    check("last_run 记录为 unsupported", cat["consumers"]["consumer-a"]["last_run"]["status"] == "unsupported")

    print("\n--- 4. consumer-a 断点续跑: 仍卡在 xz, 不重读已提交部分 ---")
    p = run("consumer", "--name", "consumer-a", data=data)
    check("续跑仍退出码 3", p.returncode == 3)
    check("续跑没有重复发出 gzip 文件的 OK", "OK " not in p.stdout, p.stdout.strip()[:120])

    print("\n--- 5. 升级 consumer-a 能力 (声明 xz) 后从断点续跑成功 ---")
    p = run("consumer", "--name", "consumer-a", "--cap", "gzip,xz", data=data)
    check("退出码 0", p.returncode == 0, p.stderr.strip())
    cat = load_catalog(data)
    check("断点累计行数 == 全量行数", cat["consumers"]["consumer-a"]["checkpoint"]["rows"] == total_raw)

    print("\n--- 6. 指定分区双向转换, 内容校验值必须不变 ---")
    before = {fid: e["content_sha256"] for fid, e in load_catalog(data)["files"].items()
              if fid.startswith("2026-11-08/")}
    p = run("ingest", "--partition", "2026-11-08", data=data)  # 已存在应报错
    check("重复入仓被拒绝", p.returncode == 1)
    env = dict(os.environ, WAREHOUSE_DATA=str(data))
    conv = subprocess.run([sys.executable, "-c",
                           "from src import ops; import json; print(json.dumps(ops.convert_partition('2026-11-08','xz')))"],
                          cwd=REPO, env=env, capture_output=True, text=True)
    check("gzip→xz 转换成功", conv.returncode == 0, conv.stderr.strip())
    mid = {fid: e["content_sha256"] for fid, e in load_catalog(data)["files"].items()
           if fid.startswith("2026-11-08/")}
    check("转换后内容校验值不变", before == mid)
    conv = subprocess.run([sys.executable, "-c",
                           "from src import ops; import json; print(json.dumps(ops.convert_partition('2026-11-08','gzip')))"],
                          cwd=REPO, env=env, capture_output=True, text=True)
    check("xz→gzip 回转成功", conv.returncode == 0, conv.stderr.strip())
    after = {fid: e["content_sha256"] for fid, e in load_catalog(data)["files"].items()
             if fid.startswith("2026-11-08/")}
    check("回转后内容校验值仍不变", before == after)

    print("\n--- 7. 损坏一个文件 -> 审计发现 -> 消费端拒读 -> 恢复 -> 全部转绿 ---")
    env = dict(os.environ, WAREHOUSE_DATA=str(data))
    subprocess.run([sys.executable, "-c", "from src import ops; ops.corrupt_file('2026-11-10/part-000')"],
                   cwd=REPO, env=env, check=True, capture_output=True, text=True)
    out = subprocess.run([sys.executable, "-c", "from src import ops; import json; print(json.dumps(ops.audit()))"],
                         cwd=REPO, env=env, capture_output=True, text=True).stdout
    rep = json.loads(out)
    check("审计 ok=False", rep["ok"] is False)
    bad = [f for f in rep["files"] if not f["ok"]]
    check("审计定位到受损文件", len(bad) == 1 and bad[0]["file"] == "2026-11-10/part-000",
          json.dumps(bad, ensure_ascii=False))
    p = run("consumer", "--name", "consumer-b", "--from-scratch", data=data)
    check("consumer-b 退出码 4 (损坏)", p.returncode == 4)
    check("consumer-b 报 CHECKSUM MISMATCH", "CHECKSUM MISMATCH" in p.stderr)
    rec = subprocess.run([sys.executable, "-c",
                          "from src import ops; import json; print(json.dumps(ops.recover_file('2026-11-10/part-000')))"],
                         cwd=REPO, env=env, capture_output=True, text=True)
    check("恢复成功", rec.returncode == 0 and '"recovered": true' in rec.stdout, rec.stderr.strip())
    out = subprocess.run([sys.executable, "-c", "from src import ops; import json; print(json.dumps(ops.audit()))"],
                         cwd=REPO, env=env, capture_output=True, text=True).stdout
    check("恢复后审计 ok=True", json.loads(out)["ok"] is True)
    p = run("consumer", "--name", "consumer-b", "--from-scratch", data=data)
    check("恢复后 consumer-b 退出码 0", p.returncode == 0)
    manifest = json.loads((data / "out" / "consumer-b.manifest.json").read_text())
    check("恢复后读到的行数仍 == raw 全量", manifest["total_rows"] == total_raw)

    print("\n--- 8. 旧解码器停用评估 ---")
    out = subprocess.run([sys.executable, "-c", "from src import ops; import json; print(json.dumps(ops.retire_check()))"],
                         cwd=REPO, env=env, capture_output=True, text=True).stdout
    r = json.loads(out)
    check("还有 gzip 存量时不可停用", r["can_retire_old_decoder"] is False)
    for part in ["2026-11-08", "2026-11-09", "2026-11-10", "2026-11-11", "2026-11-12"]:
        subprocess.run([sys.executable, "-c", f"from src import ops; ops.convert_partition('{part}','xz')"],
                       cwd=REPO, env=env, check=True, capture_output=True, text=True)
    out = subprocess.run([sys.executable, "-c", "from src import ops; import json; print(json.dumps(ops.retire_check()))"],
                         cwd=REPO, env=env, capture_output=True, text=True).stdout
    r = json.loads(out)
    check("全部转换 + 全员支持 xz + 审计通过后, 可以停用 gzip 解码器",
          r["can_retire_old_decoder"] is True, json.dumps(r["conditions"], ensure_ascii=False))
    p = run("consumer", "--name", "consumer-b", "--from-scratch", data=data)
    check("全 xz 后 consumer-b 仍读全量", p.returncode == 0 and
          json.loads((data / "out" / "consumer-b.manifest.json").read_text())["total_rows"] == total_raw)

    print()
    if FAILURES:
        print(f"共 {len(FAILURES)} 项失败: {FAILURES}")
        return 1
    print("全部通过 ✔")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
