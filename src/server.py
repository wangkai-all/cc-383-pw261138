"""网页控制台 (仅标准库 http.server)。

页面能力: 声明消费能力、转换指定分区、损坏一个文件并恢复、运行消费端、
全量核对、旧解码器停用评估。消费端以子进程方式独立运行。

用法: python -m src.server [--host 127.0.0.1] [--port 8000]
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import catalog, codecs, common, consumer, ingest, ops, seed

PAGE = """<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>压缩格式迁移控制台</title>
<style>
  :root { color-scheme: light; }
  body { font: 14px/1.5 -apple-system, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
         margin: 0; background: #f5f6f8; color: #1c2330; }
  header { background: #1c2330; color: #fff; padding: 14px 22px; }
  header h1 { font-size: 17px; margin: 0; }
  header p { margin: 4px 0 0; font-size: 12px; opacity: .75; }
  main { padding: 16px 22px 40px; display: grid; gap: 16px; }
  section { background: #fff; border: 1px solid #e2e6ec; border-radius: 10px; padding: 14px 16px; }
  h2 { font-size: 14px; margin: 0 0 10px; }
  table { border-collapse: collapse; width: 100%; font-size: 13px; }
  th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid #eef1f5; white-space: nowrap; }
  th { color: #5b6472; font-weight: 600; }
  .badge { display: inline-block; padding: 1px 8px; border-radius: 999px; font-size: 12px; font-weight: 600; }
  .fmt-gzip { background: #fde8e8; color: #b03030; }
  .fmt-xz { background: #e3f1e4; color: #22703a; }
  .ok { color: #22703a; font-weight: 600; }
  .bad { color: #b03030; font-weight: 700; }
  button { font: inherit; font-size: 12.5px; padding: 4px 10px; border: 1px solid #c9d1dc;
           border-radius: 6px; background: #fff; cursor: pointer; }
  button:hover { background: #eef3fa; }
  button.primary { background: #2563eb; border-color: #2563eb; color: #fff; }
  button.danger { color: #b03030; border-color: #e3b3b3; }
  select, input { font: inherit; font-size: 13px; padding: 3px 6px; border: 1px solid #c9d1dc; border-radius: 6px; }
  .row { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
  .mono { font-family: ui-monospace, Consolas, monospace; font-size: 12px; }
  #log { background: #10151f; color: #c9e7c9; border-radius: 8px; padding: 10px 12px;
         font: 12px/1.5 ui-monospace, Consolas, monospace; white-space: pre-wrap; max-height: 260px; overflow: auto; }
  .cond { margin: 2px 0; }
</style>
</head>
<body>
<header>
  <h1>压缩格式迁移控制台</h1>
  <p>gzip(旧) → xz(新) · 逐文件登记格式与校验值 · 消费端能力不对称 · 不支持即停, 不静默漏行</p>
</header>
<main>
  <section>
    <h2>全局配置</h2>
    <div class="row">
      <span>新写入格式: <b id="writeFmt"></b> <span class="mono" style="color:#5b6472">(只影响新写入, 存量以逐文件登记为准)</span></span>
      <select id="fmtSel"><option value="gzip">gzip</option><option value="xz">xz</option></select>
      <button onclick="setWriteFmt()">切换写入格式</button>
      <span style="flex:1"></span>
      <button onclick="seedData(false)">生成演示数据</button>
      <button class="danger" onclick="seedData(true)">重置并生成</button>
      <button class="primary" onclick="runAudit()">全量核对</button>
    </div>
    <div class="row" style="margin-top:8px">
      <span>入仓新分区:</span>
      <input id="ingPart" placeholder="分区名 如 2026-11-17" size="16">
      <input id="ingParts" type="number" value="2" min="1" max="8" style="width:60px" title="文件数">
      <button onclick="doIngest()">入仓 (按当前写入格式)</button>
      <span class="mono" style="color:#5b6472">raw/&lt;分区&gt;.jsonl 需已存在; 演示数据之外可用 --partition 指定已有 raw</span>
    </div>
  </section>

  <section>
    <h2>文件目录 (每个文件的实际格式与校验值)</h2>
    <table id="filesTbl">
      <thead><tr><th>文件</th><th>格式</th><th>行数</th><th>字节</th><th>文件 sha256</th><th>内容 sha256</th><th>实时状态</th><th>操作</th></tr></thead>
      <tbody></tbody>
    </table>
  </section>

  <section>
    <h2>消费端 (独立进程运行, 能力各自声明)</h2>
    <table id="consumersTbl">
      <thead><tr><th>名称</th><th>解码能力</th><th>断点</th><th>上次运行</th><th>操作</th></tr></thead>
      <tbody></tbody>
    </table>
  </section>

  <section>
    <h2>旧解码器 (gzip) 停用评估</h2>
    <div id="retire"></div>
  </section>

  <section>
    <h2>日志</h2>
    <div id="log">就绪。</div>
  </section>
</main>
<script>
const $ = s => document.querySelector(s);
function log(msg) {
  const el = $('#log');
  el.textContent = (typeof msg === 'string' ? msg : JSON.stringify(msg, null, 2)) + "\\n\\n" + el.textContent;
}
async function api(path, body) {
  const r = await fetch(path, {method: 'POST', headers: {'Content-Type': 'application/json'},
                               body: JSON.stringify(body || {})});
  const data = await r.json();
  if (!r.ok) { log('✗ ' + (data.error || r.statusText)); throw new Error(data.error || r.statusText); }
  return data;
}
const short = h => h ? h.slice(0, 12) + '…' : '-';

async function refresh() {
  const s = await (await fetch('/api/state')).json();
  $('#writeFmt').textContent = s.write_format;
  $('#fmtSel').value = s.write_format;
  const fb = $('#filesTbl tbody'); fb.innerHTML = '';
  for (const f of s.files) {
    const other = f.format === 'gzip' ? 'xz' : 'gzip';
    const st = f.live_status === 'OK' ? '<span class="ok">OK</span>' : '<span class="bad">' + f.live_status + '</span>';
    fb.insertAdjacentHTML('beforeend', `<tr>
      <td class="mono">${f.id}</td>
      <td><span class="badge fmt-${f.format}">${f.format}</span></td>
      <td>${f.rows}</td><td>${f.bytes}</td>
      <td class="mono" title="${f.file_sha256}">${short(f.file_sha256)}</td>
      <td class="mono" title="${f.content_sha256}">${short(f.content_sha256)}</td>
      <td>${st}</td>
      <td class="row">
        <button onclick="convertPart('${f.partition}','${other}')">转 ${other}</button>
        <button class="danger" onclick="corrupt('${f.id}')">损坏</button>
        <button onclick="recover('${f.id}')">恢复</button>
      </td></tr>`);
  }
  const cb = $('#consumersTbl tbody'); cb.innerHTML = '';
  for (const c of s.consumers) {
    const caps = ['gzip', 'xz'].map(x =>
      `<label><input type="checkbox" data-consumer="${c.name}" value="${x}" ${c.capabilities.includes(x) ? 'checked' : ''}> ${x}</label>`).join(' ');
    const ck = c.checkpoint ? `<span class="mono">${c.checkpoint.last_file} · ${c.checkpoint.rows} 行</span>` : '-';
    const lr = c.last_run ? `<span class="${c.last_run.status === 'complete' ? 'ok' : 'bad'}">${c.last_run.status}</span> <span class="mono">${c.last_run.at}</span>` : '-';
    cb.insertAdjacentHTML('beforeend', `<tr>
      <td class="mono"><b>${c.name}</b></td><td>${caps}</td><td>${ck}</td><td>${lr}</td>
      <td class="row">
        <button onclick="saveCaps('${c.name}')">保存能力</button>
        <button class="primary" onclick="runConsumer('${c.name}', true)">从头运行</button>
        <button onclick="runConsumer('${c.name}', false)">断点续跑</button>
      </td></tr>`);
  }
  const r = s.retire;
  $('#retire').innerHTML =
    `<div class="cond">${r.conditions.no_active_old_format_files.pass ? '✅' : '❌'} 无 gzip 存量文件 ` +
    `<span class="mono">${r.conditions.no_active_old_format_files.remaining.join(', ') || ''}</span></div>` +
    `<div class="cond">${r.conditions.all_consumers_support_new_format.pass ? '✅' : '❌'} 所有消费端已声明 xz 能力 ` +
    `<span class="mono">${Object.keys(r.conditions.all_consumers_support_new_format.lacking).join(', ')}</span></div>` +
    `<div class="cond">${r.conditions.audit_clean.pass ? '✅' : '❌'} 全量核对通过</div>` +
    `<div class="cond" style="margin-top:6px;font-weight:700" >结论: ${r.can_retire_old_decoder ? '<span class="ok">可以停用 gzip 解码器</span>' : '<span class="bad">暂不能停用 gzip 解码器</span>'}</div>`;
}
async function setWriteFmt() { log(await api('/api/write_format', {format: $('#fmtSel').value})); refresh(); }
async function seedData(reset) { log(await api('/api/seed', {reset})); refresh(); }
async function doIngest() {
  log(await api('/api/ingest', {partition: $('#ingPart').value.trim(), parts: +$('#ingParts').value || 1})); refresh();
}
async function convertPart(p, t) { log(await api('/api/convert', {partition: p, target: t})); refresh(); }
async function corrupt(f) { log(await api('/api/corrupt', {file: f})); refresh(); }
async function recover(f) { log(await api('/api/recover', {file: f})); refresh(); }
async function runAudit() { const r = await api('/api/audit', {}); log(r); refresh(); }
async function saveCaps(name) {
  const caps = [...document.querySelectorAll(`input[data-consumer="${name}"]:checked`)].map(i => i.value);
  if (!caps.length) { log('✗ 至少保留一种能力'); return; }
  log(await api('/api/capabilities', {name, capabilities: caps})); refresh();
}
async function runConsumer(name, fromScratch) {
  log('▶ 运行 ' + name + (fromScratch ? ' (从头)' : ' (续跑)') + ' …');
  const r = await api('/api/run_consumer', {name, from_scratch: fromScratch});
  log(`退出码 ${r.exit_code}\\n${r.stdout}${r.stderr ? '\\n[stderr]\\n' + r.stderr : ''}`);
  refresh();
}
refresh();
</script>
</body>
</html>
"""


def _state() -> dict:
    cat = catalog.load()
    files = []
    for e in catalog.active_files(cat):
        path = common.DATA_DIR / e["path"]
        if not path.exists():
            live = "MISSING"
        else:
            live = "OK" if common.file_sha256(path) == e["file_sha256"] else "DAMAGED"
        files.append({**e, "live_status": live})
    consumers = [{"name": n, **c} for n, c in sorted(cat["consumers"].items())]
    return {
        "write_format": cat["config"]["write_format"],
        "formats": {k: v["label"] for k, v in codecs.FORMATS.items()},
        "files": files,
        "consumers": consumers,
        "retire": ops.retire_check(),
    }


def _run_consumer(name: str, from_scratch: bool) -> dict:
    """消费端作为独立子进程运行。"""
    cmd = [sys.executable, "-m", "src.consumer", "--name", name]
    if from_scratch:
        cmd.append("--from-scratch")
    env = dict(os.environ, WAREHOUSE_DATA=str(common.DATA_DIR))
    proc = subprocess.run(cmd, cwd=str(common.REPO_ROOT), env=env,
                          capture_output=True, text=True, timeout=120)
    return {"name": name, "exit_code": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr,
            "meaning": {0: "读完全部", consumer.EXIT_UNSUPPORTED: "遇到不支持的格式, 已停下",
                        consumer.EXIT_CORRUPT: "数据损坏/核对失败, 已停下"}.get(proc.returncode, "未知")}


class Handler(BaseHTTPRequestHandler):
    server_version = "FormatMigration/1.0"

    def _send(self, code: int, body: bytes, ctype: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, code: int = 200) -> None:
        self._send(code, json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8"), "application/json; charset=utf-8")

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/":
            self._send(200, PAGE.encode("utf-8"), "text/html; charset=utf-8")
        elif self.path == "/api/state":
            self._json(_state())
        elif self.path == "/api/retire":
            self._json(ops.retire_check())
        else:
            self._json({"error": "not found"}, 404)

    def do_POST(self) -> None:  # noqa: N802
        try:
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length) or b"{}")
            result = self._dispatch(self.path, body)
            self._json(result)
        except (ops.OpsError, ingest.IngestError, KeyError, ValueError) as e:
            self._json({"error": str(e)}, 400)
        except subprocess.TimeoutExpired:
            self._json({"error": "消费端运行超时"}, 500)

    def _dispatch(self, path: str, body: dict):
        if path == "/api/seed":
            return seed.run(reset=bool(body.get("reset")))
        if path == "/api/ingest":
            return ingest.ingest_partition(body["partition"], int(body.get("parts") or 1),
                                           body.get("format"), bool(body.get("replace")))
        if path == "/api/write_format":
            return ops.set_write_format(body["format"])
        if path == "/api/convert":
            return ops.convert_partition(body["partition"], body["target"])
        if path == "/api/corrupt":
            return ops.corrupt_file(body["file"])
        if path == "/api/recover":
            return ops.recover_file(body["file"])
        if path == "/api/capabilities":
            caps = body["capabilities"]
            if not caps or any(c not in codecs.FORMATS for c in caps):
                raise ValueError(f"非法能力声明: {caps}")
            def work(cat: dict) -> dict:
                cat["consumers"].setdefault(body["name"], {})["capabilities"] = caps
                return {"name": body["name"], "capabilities": caps}
            return catalog.transact(work)
        if path == "/api/run_consumer":
            return _run_consumer(body["name"], bool(body.get("from_scratch")))
        if path == "/api/audit":
            return ops.audit()
        raise ValueError(f"unknown endpoint: {path}")

    def log_message(self, fmt, *args):  # 静默访问日志
        pass


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args(argv)
    common.ensure_dirs()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"控制台: http://{args.host}:{args.port}  (数据目录: {common.DATA_DIR})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
