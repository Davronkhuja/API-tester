import json
import os
import re
import time
import datetime
import threading
import webbrowser
import uuid
import queue as queue_module

import requests as req_lib
from flask import Flask, request, jsonify, Response, stream_with_context

app = Flask(__name__)

# ============================================================
# SAVED DATA  (saved_requests.json)
# Format: {"folders": [...], "requests": [...]}
# ============================================================

SAVED_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "saved_requests.json"
)


def load_db():
    try:
        with open(SAVED_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):           # migrate old format
                data = {"folders": [], "requests": data}
            data.setdefault("folders", [])
            data.setdefault("requests", [])
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        return {"folders": [], "requests": []}


def save_db(data):
    with open(SAVED_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def now_iso():
    return datetime.datetime.now().isoformat()


# ============================================================
# JOB MANAGEMENT
# ============================================================

active_jobs = {}
active_jobs_lock = threading.Lock()


# ============================================================
# VARIABLE REPLACEMENT
# ============================================================

def replace_variables(text, row):
    if text is None:
        return ""
    def replacer(match):
        key = match.group(1).strip()
        if key in row:
            v = row[key]
            return "" if v is None else str(v)
        return match.group(0)
    return re.sub(r"\{\{\s*([^}]+?)\s*\}\}", replacer, str(text))


# ============================================================
# JOB WORKER
# ============================================================

def job_worker(job_id):
    with active_jobs_lock:
        job = active_jobs.get(job_id)
        if not job:
            return

    config       = job["config"]
    result_queue = job["queue"]
    stop_event   = job["stop_event"]

    rows        = config["rows"]
    method      = config.get("method", "GET").upper()
    url_tmpl    = config.get("url", "")
    auth_tmpl   = config.get("authorization", "")
    params_tmpl = config.get("params", [])
    hdrs_tmpl   = config.get("headers", [])
    body_tmpl   = config.get("body", "")
    ctype       = config.get("content_type", "")
    body_type   = config.get("body_type", "raw")
    delay       = float(config.get("delay", 0))

    successful = 0
    failed     = 0
    times      = []
    index      = 0

    for index, row in enumerate(rows, start=1):
        if stop_event.is_set():
            break

        url  = replace_variables(url_tmpl, row)
        auth = replace_variables(auth_tmpl, row)

        hdrs = {}
        if auth:
            hdrs["Authorization"] = auth
        for item in hdrs_tmpl:
            name = item.get("name", "").strip()
            val  = item.get("value", "")
            if name:
                hdrs[name] = replace_variables(val, row)
        if body_tmpl and ctype and "content-type" not in {k.lower() for k in hdrs}:
            hdrs["Content-Type"] = ctype

        params = {}
        for item in params_tmpl:
            name = item.get("name", "").strip()
            val  = item.get("value", "")
            if name:
                params[name] = replace_variables(val, row)

        body = replace_variables(body_tmpl, row)

        start = time.perf_counter()
        try:
            kw = {"params": params, "headers": hdrs, "timeout": 120}
            if method in ("POST", "PUT", "PATCH") and body:
                if body_type == "json":
                    kw["json"] = json.loads(body)
                else:
                    kw["data"] = body.encode("utf-8")
            elif method == "DELETE" and body:
                kw["data"] = body.encode("utf-8")

            resp    = req_lib.request(method, url, **kw)
            elapsed = time.perf_counter() - start
            try:
                resp_body = resp.json()
            except Exception:
                resp_body = resp.text

            status = resp.status_code
            ok     = 200 <= status < 300
            if ok:
                successful += 1
            else:
                failed += 1

            result = {
                "index": index, "total": len(rows),
                "status": status, "time": elapsed,
                "size": len(resp.content), "url": resp.url,
                "response": resp_body, "error": not ok,
                "successful": successful, "failed": failed,
            }

        except req_lib.Timeout:
            elapsed = time.perf_counter() - start
            failed += 1
            result = {
                "index": index, "total": len(rows),
                "status": "TIMEOUT", "time": elapsed,
                "size": 0, "url": url,
                "response": "Request timed out (120s)", "error": True,
                "successful": successful, "failed": failed,
            }
        except Exception as exc:
            elapsed = time.perf_counter() - start
            failed += 1
            result = {
                "index": index, "total": len(rows),
                "status": "ERROR", "time": elapsed,
                "size": 0, "url": url,
                "response": str(exc), "error": True,
                "successful": successful, "failed": failed,
            }

        times.append(result["time"])
        result_queue.put({"type": "result", "data": result})

        if index < len(rows) and delay > 0 and not stop_event.is_set():
            steps = int(delay / 0.1)
            for _ in range(steps):
                if stop_event.is_set():
                    break
                time.sleep(0.1)
            leftover = delay - steps * 0.1
            if leftover > 0 and not stop_event.is_set():
                time.sleep(leftover)

    summary = {
        "total": len(rows), "completed": index,
        "successful": successful, "failed": failed,
        "min_time": min(times) if times else 0,
        "max_time": max(times) if times else 0,
        "avg_time": sum(times) / len(times) if times else 0,
        "stopped": stop_event.is_set(),
    }
    result_queue.put({"type": "done", "data": summary})


# ============================================================
# HTML
# ============================================================

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>API Runner — Batch so'rov yuborish vositasi</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 36 36'%3E%3Cdefs%3E%3ClinearGradient id='bg' x1='0' y1='0' x2='36' y2='36' gradientUnits='userSpaceOnUse'%3E%3Cstop offset='0%25' stop-color='%232563eb'/%3E%3Cstop offset='100%25' stop-color='%237c3aed'/%3E%3C/linearGradient%3E%3ClinearGradient id='bolt' x1='11' y1='5' x2='25' y2='31' gradientUnits='userSpaceOnUse'%3E%3Cstop offset='0%25' stop-color='%23ffffff'/%3E%3Cstop offset='100%25' stop-color='%23bfdbfe'/%3E%3C/linearGradient%3E%3C/defs%3E%3Crect width='36' height='36' rx='9' fill='url(%23bg)'/%3E%3Crect width='36' height='36' rx='9' fill='white' opacity='0.06'/%3E%3Cpath d='M22,5 L11,20 L19,20 L14,31 L25,16 L17,16 Z' fill='url(%23bolt)'/%3E%3C/svg%3E">
<style>
/* ── RESET & ROOT ─────────────────────────────────────────── */
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
:root {
  --primary:    #4F7EF7;
  --primary-d:  #3B6DEA;
  --primary-bg: #EEF3FF;
  --success:    #16A34A;
  --success-bg: #F0FDF4;
  --error:      #DC2626;
  --error-bg:   #FEF2F2;
  --warning:    #D97706;
  --warning-bg: #FFFBEB;
  --bg:         #F3F6FB;
  --surface:    #FFFFFF;
  --border:     #E5E7EB;
  --border-d:   #D1D5DB;
  --text:       #111827;
  --muted:      #6B7280;
  --light:      #9CA3AF;
  --hdr:        #1A2336;
  --sb:         #151D2E;
  --sb2:        #1E2840;
  --mono: 'JetBrains Mono','Cascadia Code','Fira Code',Consolas,monospace;
  --sans: -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  --rad:   8px;
  --rad-lg:12px;
  --sh:    0 1px 3px rgba(0,0,0,.1),0 1px 2px rgba(0,0,0,.06);
  --sh-md: 0 4px 6px rgba(0,0,0,.07),0 2px 4px rgba(0,0,0,.05);
}
body {
  font-family: var(--sans);
  background: var(--bg);
  color: var(--text);
  font-size: 14px;
  line-height: 1.5;
  min-height: 100vh;
}
input,select,textarea,button { font-family: inherit; font-size: 14px; }

/* ── HEADER ───────────────────────────────────────────────── */
.app-header {
  background: var(--hdr);
  padding: 0 20px;
  height: 58px;
  display: flex;
  align-items: center;
  gap: 12px;
  position: sticky;
  top: 0;
  z-index: 200;
  box-shadow: 0 2px 12px rgba(0,0,0,.35);
  border-bottom: 1px solid rgba(255,255,255,.06);
}
.app-logo { display: flex; align-items: center; gap: 10px; }
.app-logo svg { flex-shrink: 0; filter: drop-shadow(0 2px 6px rgba(79,126,247,.5)); }
.app-name-group { display: flex; flex-direction: column; gap: 1px; }
.app-name  { font-weight: 800; font-size: 16px; letter-spacing: -.4px; color: #fff; line-height: 1; }
.app-sub   { font-size: 10.5px; color: rgba(255,255,255,.4); line-height: 1; }
.app-ver   {
  background: rgba(79,126,247,.3); border: 1px solid rgba(79,126,247,.45);
  color: #93B4FC; font-size: 9.5px; font-weight: 700;
  padding: 2px 6px; border-radius: 100px; letter-spacing: .6px; text-transform: uppercase;
}
.hdr-space { flex: 1; }
.hdr-chips { display: flex; gap: 7px; }
.hdr-chip {
  display: flex; align-items: center; gap: 5px;
  font-size: 11px; color: rgba(255,255,255,.35);
  background: rgba(255,255,255,.05); border: 1px solid rgba(255,255,255,.09);
  padding: 3px 9px; border-radius: 100px; white-space: nowrap;
}

/* ── LAYOUT ───────────────────────────────────────────────── */
.layout {
  display: flex;
  height: calc(100vh - 58px);
  overflow: hidden;
}

/* Content area (left pane + resizer + results pane) */
.content-area {
  display: flex;
  flex: 1;
  min-width: 0;
  overflow: hidden;
}

/* ── MAIN SCROLL ─────────────────────────────────────────── */

/* ── SIDEBAR ──────────────────────────────────────────────── */
.sidebar {
  width: 256px;
  flex-shrink: 0;
  background: var(--sb);
  border-right: 1px solid rgba(255,255,255,.05);
  display: flex;
  flex-direction: column;
  overflow: hidden;
  transition: width .22s cubic-bezier(.4,0,.2,1);
}
.sidebar.closed { width: 0; border-right: none; }
.sb-resizer {
  width: 5px; flex-shrink: 0;
  background: var(--border);
  cursor: col-resize;
  position: relative;
  transition: background .15s;
}
.sb-resizer:hover, .sb-resizer.dragging { background: var(--primary); }
.sb-resizer-toggle {
  position: absolute; top: 50%; right: -12px;
  transform: translateY(-50%);
  width: 20px; height: 36px;
  display: flex; align-items: center; justify-content: center;
  background: var(--surface); border: 1px solid var(--border-d);
  border-left: none; border-radius: 0 6px 6px 0;
  cursor: pointer; font-size: 10px; color: var(--muted);
  z-index: 10; transition: all .15s;
}
.sb-resizer-toggle:hover { background: var(--primary); color: #fff; border-color: var(--primary); }

/* Top action buttons */
.sb-actions {
  display: flex;
  gap: 6px;
  padding: 10px 10px 0;
}
.sb-btn {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 5px;
  padding: 7px 6px;
  border-radius: 6px;
  border: 1px solid;
  cursor: pointer;
  font-size: 11.5px;
  font-weight: 600;
  transition: all .15s;
  white-space: nowrap;
}
.sb-btn-req {
  background: rgba(79,126,247,.18);
  border-color: rgba(79,126,247,.35);
  color: #93B4FC;
}
.sb-btn-req:hover { background: rgba(79,126,247,.3); border-color: rgba(79,126,247,.6); color: #fff; }
.sb-btn-folder {
  background: rgba(255,255,255,.05);
  border-color: rgba(255,255,255,.1);
  color: rgba(255,255,255,.5);
}
.sb-btn-folder:hover { background: rgba(255,255,255,.1); border-color: rgba(255,255,255,.2); color: rgba(255,255,255,.85); }

/* Search */
.sb-search-wrap {
  position: relative;
  padding: 8px 10px;
}
.sb-search {
  width: 100%;
  padding: 6px 10px 6px 28px;
  border-radius: 6px;
  border: 1px solid rgba(255,255,255,.1);
  background: rgba(255,255,255,.06);
  color: rgba(255,255,255,.8);
  font-size: 12px;
  outline: none;
}
.sb-search::placeholder { color: rgba(255,255,255,.22); }
.sb-search:focus { border-color: rgba(79,126,247,.55); background: rgba(255,255,255,.09); }
.sb-search-ico {
  position: absolute; left: 18px; top: 50%; transform: translateY(-50%);
  color: rgba(255,255,255,.22); pointer-events: none;
}

/* Tree */
.sb-tree {
  flex: 1;
  overflow-y: auto;
  padding: 0 6px 12px;
  scrollbar-width: thin;
  scrollbar-color: rgba(255,255,255,.1) transparent;
}
.sb-tree::-webkit-scrollbar { width: 4px; }
.sb-tree::-webkit-scrollbar-thumb { background: rgba(255,255,255,.1); border-radius: 4px; }

/* Folder */
.sb-folder { margin-bottom: 2px; }
.sb-folder-head {
  display: flex;
  align-items: center;
  gap: 5px;
  padding: 7px 8px;
  border-radius: 6px;
  cursor: pointer;
  transition: background .12s;
  position: relative;
}
.sb-folder-head:hover { background: rgba(255,255,255,.07); }
.sb-chevron { font-size: 9px; color: rgba(255,255,255,.3); width: 10px; flex-shrink: 0; transition: transform .15s; }
.sb-folder-ico { font-size: 13px; flex-shrink: 0; }
.sb-folder-name {
  flex: 1;
  font-size: 12.5px;
  font-weight: 600;
  color: rgba(255,255,255,.75);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.sb-folder-count {
  font-size: 10px;
  color: rgba(255,255,255,.25);
  background: rgba(255,255,255,.07);
  padding: 1px 5px;
  border-radius: 100px;
  font-family: var(--mono);
}
.sb-folder-acts { display: none; gap: 1px; }
.sb-folder-head:hover .sb-folder-acts { display: flex; }
.sb-folder-body { padding-left: 14px; }

/* Request item */
.sb-item {
  display: flex;
  align-items: center;
  gap: 7px;
  padding: 7px 8px;
  border-radius: 6px;
  cursor: pointer;
  transition: background .12s;
  position: relative;
}
.sb-item:hover { background: rgba(255,255,255,.06); }
.sb-item.active { background: rgba(79,126,247,.17); }
.sb-item.active::before {
  content: '';
  position: absolute;
  left: 0; top: 5px; bottom: 5px;
  width: 3px;
  background: var(--primary);
  border-radius: 0 3px 3px 0;
}
.sb-item-info { flex: 1; min-width: 0; }
.sb-item-name {
  font-size: 12px; font-weight: 500;
  color: rgba(255,255,255,.78);
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.sb-item.active .sb-item-name { color: #fff; }
.sb-item-url {
  font-size: 10px; color: rgba(255,255,255,.25);
  font-family: var(--mono);
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  margin-top: 1px;
}
.sb-item-acts { display: none; gap: 1px; flex-shrink: 0; }
.sb-item:hover .sb-item-acts { display: flex; }
.sb-act {
  padding: 2px 4px;
  background: none; border: none;
  border-radius: 3px; cursor: pointer;
  color: rgba(255,255,255,.3); font-size: 12px; line-height: 1;
  transition: all .12s;
}
.sb-act:hover { background: rgba(255,255,255,.1); color: rgba(255,255,255,.85); }
.sb-act.del:hover { background: rgba(220,38,38,.25); color: #FCA5A5; }

/* Method pills */
.mpill {
  font-size: 8.5px; font-weight: 800; letter-spacing: .3px;
  padding: 2px 5px; border-radius: 3px; text-transform: uppercase;
  flex-shrink: 0; min-width: 34px; text-align: center;
  font-family: var(--mono);
}
.mpill-GET    { background: rgba(22,163,74,.2);  color: #4ADE80; }
.mpill-POST   { background: rgba(79,126,247,.2); color: #93B4FC; }
.mpill-PUT    { background: rgba(217,119,6,.2);  color: #FCD34D; }
.mpill-PATCH  { background: rgba(139,92,246,.2); color: #C4B5FD; }
.mpill-DELETE { background: rgba(220,38,38,.2);  color: #FCA5A5; }

/* Divider */
.sb-divider {
  font-size: 9.5px; font-weight: 700; letter-spacing: .8px;
  text-transform: uppercase; color: rgba(255,255,255,.2);
  padding: 10px 10px 4px;
  display: flex; align-items: center; gap: 6px;
}
.sb-divider::after {
  content: ''; flex: 1; height: 1px;
  background: rgba(255,255,255,.07);
}

/* Empty */
.sb-empty {
  text-align: center; padding: 28px 14px;
  color: rgba(255,255,255,.2); font-size: 12px; line-height: 1.7;
}
.sb-empty-ico { font-size: 26px; opacity: .35; margin-bottom: 8px; }

/* Inline rename input */
.sb-rename-input {
  background: rgba(255,255,255,.1);
  border: 1px solid rgba(79,126,247,.6);
  border-radius: 4px; color: #fff;
  font-size: 12px; font-weight: 500;
  width: 100%; padding: 1px 5px; outline: none;
}

/* ── MAIN SCROLL ─────────────────────────────────────────── */
.main-scroll {
  flex: 1;
  min-width: 320px;
  overflow-y: auto;
  padding: 20px 22px 60px;
  display: flex;
  flex-direction: column;
  gap: 18px;
}

/* ── RESIZER ─────────────────────────────────────────────── */
.resizer {
  width: 5px;
  background: var(--border);
  cursor: col-resize;
  flex-shrink: 0;
  position: relative;
  transition: background .12s;
  z-index: 10;
  user-select: none;
}
.resizer:hover,
.resizer.dragging { background: var(--primary); }

.resizer-toggle {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  width: 18px;
  height: 48px;
  background: var(--surface);
  border: 1px solid var(--border-d);
  border-radius: 10px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 9px;
  color: var(--muted);
  box-shadow: var(--sh-md);
  transition: all .15s;
}
.resizer-toggle:hover { background: var(--primary); color: #fff; border-color: var(--primary); }

/* ── RESULTS PANE ────────────────────────────────────────── */
.results-pane {
  width: 520px;
  min-width: 0;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  background: var(--bg);
  border-left: 1px solid var(--border);
  flex-shrink: 0;
  transition: width .22s cubic-bezier(.4,0,.2,1);
}
.results-pane.closed {
  width: 0 !important;
  border-left: none;
}

.rp-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 11px 14px;
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  flex-shrink: 0;
  position: sticky;
  top: 0;
  z-index: 5;
  min-width: 0;
  white-space: nowrap;
}
.rp-title {
  font-weight: 700; font-size: 12px;
  text-transform: uppercase; letter-spacing: .7px; color: var(--muted);
}
.rp-spacer { flex: 1; }
.rp-close {
  width: 26px; height: 26px;
  display: flex; align-items: center; justify-content: center;
  background: none; border: 1px solid var(--border-d); border-radius: 6px;
  cursor: pointer; font-size: 16px; color: var(--muted);
  transition: all .12s; line-height: 1;
}
.rp-close:hover { background: var(--error-bg); border-color: var(--error); color: var(--error); }

.rp-body {
  flex: 1;
  overflow-y: auto;
  padding: 14px 14px 40px;
  min-width: 0;
}

/* ── CARDS ────────────────────────────────────────────────── */
.card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 12px;
  box-shadow: var(--sh);
  overflow: visible;
}
.card-header {
  display: flex; align-items: center; gap: 10px;
  padding: 13px 18px;
  border-bottom: 1px solid var(--border);
  background: #FAFBFC;
  border-radius: 12px 12px 0 0;
}
.card-title {
  font-weight: 700; font-size: 12px;
  text-transform: uppercase; letter-spacing: .7px; color: var(--muted);
}
.card-acts { margin-left: auto; display: flex; gap: 7px; }
.btn-curl {
  display: flex; align-items: center; gap: 5px;
  padding: 5px 13px;
  background: transparent; color: var(--muted);
  border: 1px solid var(--border-d); border-radius: 6px;
  cursor: pointer; font-size: 12px; font-weight: 600;
  transition: all .15s;
}
.btn-curl:hover { background: var(--surface-d,#f1f3f6); color: var(--text); border-color: var(--muted); }
.btn-curl.copied { background: #dcfce7; color: #16a34a; border-color: #86efac; }
.card-body { padding: 18px; }

/* Save button (in card header) */
.btn-save {
  display: flex; align-items: center; gap: 5px;
  padding: 5px 13px;
  background: var(--primary); color: #fff;
  border: none; border-radius: 6px;
  cursor: pointer; font-size: 12px; font-weight: 600;
  transition: all .15s;
}
.btn-save:hover:not(:disabled) { background: var(--primary-d); box-shadow: 0 3px 10px rgba(79,126,247,.35); }
.btn-save:disabled { background: var(--border-d); color: var(--muted); cursor: default; box-shadow: none; }

/* ── URL BAR ──────────────────────────────────────────────── */
.url-bar { display: flex; gap: 8px; align-items: center; }
.method-sel {
  width: 106px; padding: 8px 10px;
  border: 1px solid var(--border-d); border-radius: var(--rad);
  background: var(--bg); font-weight: 700; color: var(--primary);
  cursor: pointer; outline: none; appearance: none;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='7'%3E%3Cpath d='M1 1l5 5 5-5' stroke='%236B7280' stroke-width='1.5' fill='none' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E");
  background-repeat: no-repeat; background-position: right 9px center; padding-right: 26px;
}
.method-sel:focus { border-color: var(--primary); }
.url-input {
  flex: 1; padding: 8px 11px;
  border: 1px solid var(--border-d); border-radius: var(--rad);
  font-family: var(--mono); font-size: 12.5px; outline: none;
  transition: border-color .15s;
}
.url-input:focus { border-color: var(--primary); box-shadow: 0 0 0 3px rgba(79,126,247,.1); }
.url-run-btn { padding: 8px 16px; font-size: 13px; white-space: nowrap; flex-shrink: 0; }

/* ── TABS ─────────────────────────────────────────────────── */
.tabs { display: flex; gap: 2px; margin-top: 14px; border-bottom: 1px solid var(--border); }
.tab-btn {
  padding: 7px 14px; background: none; border: none;
  cursor: pointer; font-weight: 500; color: var(--muted);
  border-bottom: 2px solid transparent; margin-bottom: -1px;
  transition: color .15s, border-color .15s;
  border-radius: var(--rad) var(--rad) 0 0; font-size: 13px;
}
.tab-btn:hover { color: var(--text); background: var(--bg); }
.tab-btn.active { color: var(--primary); border-bottom-color: var(--primary); font-weight: 600; }
.tab-panel { display: none; padding-top: 14px; }
.tab-panel.active { display: block; }

/* ── KV TABLE ─────────────────────────────────────────────── */
.kv-table { width: 100%; border-collapse: collapse; }
.kv-table th {
  text-align: left; padding: 5px 9px;
  background: var(--bg); border: 1px solid var(--border);
  font-size: 11px; font-weight: 600; color: var(--muted);
  text-transform: uppercase; letter-spacing: .5px;
}
.kv-table td { border: 1px solid var(--border); padding: 1px; }
.kv-table input {
  width: 100%; padding: 6px 9px; border: none; outline: none;
  background: transparent; font-family: var(--mono); font-size: 12.5px;
}
.kv-table input:focus { background: var(--primary-bg); }
.kv-del { width: 34px; text-align: center; cursor: pointer; color: var(--error); font-size: 17px; font-weight: bold; user-select: none; opacity: .45; }
.kv-del:hover { opacity: 1; }
.btn-add-row {
  margin-top: 7px; padding: 4px 11px;
  background: none; border: 1px dashed var(--border-d); border-radius: var(--rad);
  cursor: pointer; color: var(--muted); font-size: 12px; transition: all .15s;
}
.btn-add-row:hover { border-color: var(--primary); color: var(--primary); background: var(--primary-bg); }

/* ── AUTH / BODY ──────────────────────────────────────────── */
.field-label {
  font-size: 11px; font-weight: 600; color: var(--muted);
  text-transform: uppercase; letter-spacing: .5px; margin-bottom: 5px;
}
.text-input {
  width: 100%; padding: 8px 11px;
  border: 1px solid var(--border-d); border-radius: var(--rad);
  outline: none; font-family: var(--mono); font-size: 12.5px; transition: border-color .15s;
}
.text-input:focus { border-color: var(--primary); box-shadow: 0 0 0 3px rgba(79,126,247,.1); }
.field-hint { font-size: 11.5px; color: var(--light); margin-top: 5px; }
.body-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 11px; }
.select-input {
  width: 100%; padding: 8px 10px;
  border: 1px solid var(--border-d); border-radius: var(--rad);
  outline: none; cursor: pointer; appearance: none;
  background: #fff url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='7'%3E%3Cpath d='M1 1l5 5 5-5' stroke='%236B7280' stroke-width='1.5' fill='none' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E") no-repeat right 9px center;
  padding-right: 26px;
}
.select-input:focus { border-color: var(--primary); }
.body-wrap { position: relative; }
.body-textarea {
  width: 100%; min-height: 150px; max-height: 60vh; padding: 11px;
  border: 1px solid var(--border-d); border-radius: var(--rad);
  font-family: var(--mono); font-size: 12.5px; outline: none; resize: vertical; line-height: 1.6;
  box-sizing: border-box;
}
.body-textarea:focus { border-color: var(--primary); box-shadow: 0 0 0 3px rgba(79,126,247,.1); }
.btn-beautify {
  position: absolute; top: 7px; right: 9px;
  padding: 3px 9px; font-size: 11px; font-weight: 600; letter-spacing: .3px;
  border: 1px solid var(--border-d); border-radius: 5px;
  background: var(--surface); color: var(--muted); cursor: pointer;
  transition: all .15s; display: none;
}
.btn-beautify:hover { background: var(--primary); color: #fff; border-color: var(--primary); }
.btn-beautify.visible { display: block; }

/* ── FILE UPLOAD ──────────────────────────────────────────── */
.upload-zone {
  border: 2px dashed var(--border-d); border-radius: var(--rad-lg);
  padding: 28px 20px; text-align: center; cursor: pointer;
  transition: all .2s; background: var(--bg); position: relative;
}
.upload-zone:hover, .upload-zone.drag-over { border-color: var(--primary); background: var(--primary-bg); }
.upload-zone input[type="file"] { position: absolute; inset: 0; opacity: 0; cursor: pointer; }
.upload-ico   { font-size: 32px; margin-bottom: 8px; }
.upload-title { font-weight: 600; margin-bottom: 3px; }
.upload-sub   { font-size: 11.5px; color: var(--muted); }
.file-bar {
  display: flex; align-items: center; gap: 11px;
  padding: 9px 13px; margin-top: 11px;
  background: var(--success-bg); border: 1px solid #BBF7D0; border-radius: var(--rad);
}
.file-bar-name { font-weight: 600; color: var(--success); font-size: 13px; }
.file-bar-meta { font-size: 11.5px; color: var(--muted); }
.file-bar-clear {
  margin-left: auto; background: none; border: none; cursor: pointer;
  color: var(--muted); font-size: 17px; padding: 1px 5px; border-radius: 4px; transition: all .15s;
}
.file-bar-clear:hover { background: rgba(0,0,0,.07); color: var(--error); }
.preview-wrap { margin-top: 12px; border: 1px solid var(--border); border-radius: var(--rad); overflow: auto; max-height: 180px; }
.preview-table { width: 100%; border-collapse: collapse; font-size: 11.5px; }
.preview-table th { background: #F3F4F6; padding: 5px 9px; border-bottom: 1px solid var(--border); text-align: left; font-weight: 600; color: var(--muted); position: sticky; top: 0; white-space: nowrap; font-family: var(--mono); }
.preview-table td { padding: 4px 9px; border-bottom: 1px solid var(--border); font-family: var(--mono); max-width: 160px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.preview-table tr:last-child td { border-bottom: none; }
.preview-table tr:hover td { background: var(--primary-bg); }
.preview-more { text-align: center; padding: 5px; font-size: 11px; color: var(--muted); background: #FAFBFC; border-top: 1px solid var(--border); }

/* ── RUNNER SETTINGS ──────────────────────────────────────── */
.runner-grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; margin-top: 14px; }

/* ── BUTTONS ──────────────────────────────────────────────── */
.btn {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 8px 18px; border: none; border-radius: var(--rad);
  cursor: pointer; font-weight: 600; font-size: 13px; transition: all .15s;
}
.btn:disabled { opacity: .45; cursor: not-allowed; pointer-events: none; }
.btn-primary { background: var(--primary); color: #fff; }
.btn-primary:hover { background: var(--primary-d); box-shadow: 0 4px 12px rgba(79,126,247,.4); }
.btn-danger  { background: var(--error);   color: #fff; }
.btn-danger:hover  { background: #B91C1C; }
.btn-ghost   { background: none; border: 1px solid var(--border-d); color: var(--muted); }
.btn-ghost:hover { border-color: var(--text); color: var(--text); background: var(--bg); }
.btn-sm { padding: 5px 11px; font-size: 11.5px; }
.action-row { display: flex; gap: 9px; margin-top: 18px; }

/* ── PROGRESS ─────────────────────────────────────────────── */
.prog-head { display: flex; align-items: center; justify-content: space-between; margin-bottom: 7px; }
.prog-label { font-weight: 600; font-size: 12.5px; }
.prog-count { font-size: 12.5px; color: var(--muted); font-family: var(--mono); }
.prog-bar { height: 7px; background: #E5E7EB; border-radius: 100px; overflow: hidden; }
.prog-fill {
  height: 100%; border-radius: 100px; width: 0%;
  background: linear-gradient(90deg, var(--primary), #7C3AED);
  transition: width .3s ease;
}
.prog-fill.done    { background: linear-gradient(90deg, var(--success), #22C55E); }
.prog-fill.stopped { background: linear-gradient(90deg, var(--warning), #F59E0B); }

/* ── STATS ────────────────────────────────────────────────── */
.stats-grid { display: grid; grid-template-columns: repeat(4,1fr); gap: 11px; margin: 14px 0; }
.rp-body .stats-grid { grid-template-columns: repeat(2,1fr); }
.stat-card { background: var(--bg); border: 1px solid var(--border); border-radius: var(--rad); padding: 12px 14px; text-align: center; }
.stat-card.s-ok   { background: var(--success-bg); border-color: #BBF7D0; }
.stat-card.s-err  { background: var(--error-bg);   border-color: #FECACA; }
.stat-card.s-time { background: #F0F9FF; border-color: #BAE6FD; }
.stat-val { font-size: 24px; font-weight: 800; line-height: 1; font-family: var(--mono); }
.stat-card.s-ok   .stat-val { color: var(--success); }
.stat-card.s-err  .stat-val { color: var(--error); }
.stat-card.s-time .stat-val { color: #0369A1; font-size: 18px; }
.stat-lbl { font-size: 10.5px; color: var(--muted); font-weight: 600; text-transform: uppercase; letter-spacing: .5px; margin-top: 3px; }

/* ── STATUS BAR ───────────────────────────────────────────── */
.status-bar {
  display: flex; align-items: center; gap: 7px;
  padding: 9px 13px; background: var(--bg);
  border: 1px solid var(--border); border-radius: var(--rad);
  font-size: 12.5px; color: var(--muted); margin-bottom: 14px;
}
.status-dot { width: 7px; height: 7px; border-radius: 50%; background: var(--light); flex-shrink: 0; }
.status-dot.running { background: var(--primary); animation: pulse 1s infinite; }
.status-dot.done    { background: var(--success); }
.status-dot.error   { background: var(--error); }
.status-dot.stopped { background: var(--warning); }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.35} }

/* ── FILTER ───────────────────────────────────────────────── */
.filter-bar { display: flex; gap: 5px; margin-bottom: 12px; }
.flt-btn {
  padding: 4px 12px; border-radius: 100px;
  border: 1px solid var(--border-d); background: none;
  cursor: pointer; font-size: 11.5px; font-weight: 500; color: var(--muted); transition: all .15s;
}
.flt-btn.active            { background: var(--text);    color: #fff; border-color: var(--text); }
.flt-btn.f-ok.active  { background: var(--success); color: #fff; border-color: var(--success); }
.flt-btn.f-err.active { background: var(--error);   color: #fff; border-color: var(--error); }
.flt-spacer { flex: 1; }

/* ── RESULT CARDS ─────────────────────────────────────────── */
.result-card { border: 1px solid var(--border); border-radius: var(--rad); margin-bottom: 7px; overflow: hidden; }
.result-card.is-ok  { border-left: 4px solid var(--success); }
.result-card.is-err { border-left: 4px solid var(--error); }
.result-head {
  display: flex; align-items: center; gap: 8px;
  padding: 9px 13px; cursor: pointer; background: #FAFBFC; transition: background .12s;
}
.result-head:hover { background: #F3F4F6; }
.result-num { font-size: 10.5px; font-weight: 700; color: var(--muted); font-family: var(--mono); min-width: 24px; }
.status-badge { font-size: 11.5px; font-weight: 700; padding: 2px 7px; border-radius: 100px; font-family: var(--mono); }
.b-2xx { background: var(--success-bg); color: var(--success); }
.b-4xx, .b-5xx { background: var(--error-bg); color: var(--error); }
.b-err { background: var(--warning-bg); color: var(--warning); }
.result-url  { font-size: 11.5px; color: var(--muted); font-family: var(--mono); flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.result-time { font-size: 11.5px; color: var(--light); font-family: var(--mono); white-space: nowrap; }
.result-sz   { font-size: 10.5px; color: var(--light); white-space: nowrap; }
.chevron { font-size: 9px; color: var(--light); transition: transform .18s; }
.result-card.expanded .chevron { transform: rotate(180deg); }
.result-body { display: none; padding: 13px; border-top: 1px solid var(--border); }
.result-card.expanded .result-body { display: block; }
.result-fl { font-size: 10.5px; font-weight: 700; color: var(--muted); text-transform: uppercase; letter-spacing: .5px; margin: 9px 0 4px; }
.result-fl:first-child { margin-top: 0; }
.url-display {
  font-family: var(--mono); font-size: 11.5px; padding: 7px 11px;
  background: var(--bg); border: 1px solid var(--border); border-radius: var(--rad); word-break: break-all;
}
.resp-wrap { position: relative; }
.copy-btn {
  position: absolute; top: 7px; right: 7px;
  padding: 2px 9px; font-size: 10.5px;
  background: rgba(255,255,255,.92); border: 1px solid var(--border);
  border-radius: var(--rad); cursor: pointer; font-weight: 500; color: var(--muted);
  backdrop-filter: blur(4px); transition: all .15s;
}
.copy-btn:hover { border-color: var(--primary); color: var(--primary); }
.copy-btn.copied { color: var(--success); border-color: var(--success); }
pre.resp-pre {
  background: #0D1117; color: #C9D1D9;
  padding: 13px 15px; border-radius: var(--rad);
  overflow: auto; max-height: 380px;
  font-family: var(--mono); font-size: 12px; line-height: 1.6;
  white-space: pre-wrap; word-break: break-word;
}
.j-key  { color: #79C0FF; }
.j-str  { color: #A5D6FF; }
.j-num  { color: #F0883E; }
.j-bool { color: #FF7B72; }
.j-null { color: #FF7B72; }

/* ── EMPTY ────────────────────────────────────────────────── */
.empty-state { text-align: center; padding: 36px 20px; color: var(--muted); }
.empty-ico { font-size: 36px; margin-bottom: 8px; opacity: .45; }

/* ── MODALS ───────────────────────────────────────────────── */
.modal-overlay {
  position: fixed; inset: 0; background: rgba(0,0,0,.55);
  z-index: 500; display: flex; align-items: center; justify-content: center;
  backdrop-filter: blur(3px);
}
.modal-box {
  background: #1E2840; border: 1px solid rgba(255,255,255,.11);
  border-radius: 14px; padding: 22px; width: 380px;
  box-shadow: 0 20px 60px rgba(0,0,0,.5);
}
.modal-title { font-size: 15px; font-weight: 700; color: #fff; margin-bottom: 16px; }
.modal-field { margin-bottom: 12px; }
.modal-label { font-size: 11px; font-weight: 600; color: rgba(255,255,255,.4); text-transform: uppercase; letter-spacing: .5px; margin-bottom: 5px; }
.modal-input, .modal-select {
  width: 100%; padding: 9px 11px;
  background: rgba(255,255,255,.07); border: 1px solid rgba(255,255,255,.13);
  border-radius: var(--rad); color: #fff; font-size: 13px; outline: none;
}
.modal-input::placeholder { color: rgba(255,255,255,.28); }
.modal-input:focus, .modal-select:focus { border-color: rgba(79,126,247,.65); background: rgba(255,255,255,.1); }
.modal-select option { background: #1E2840; color: #fff; }
.modal-hint { font-size: 11px; color: rgba(255,255,255,.3); margin-top: 5px; }
.modal-btns { display: flex; gap: 7px; margin-top: 18px; justify-content: flex-end; }
.modal-cancel {
  padding: 7px 15px; background: rgba(255,255,255,.07);
  border: 1px solid rgba(255,255,255,.11); border-radius: var(--rad);
  color: rgba(255,255,255,.55); cursor: pointer; font-size: 12.5px; font-weight: 500; transition: all .15s;
}
.modal-cancel:hover { background: rgba(255,255,255,.12); color: #fff; }
.modal-confirm {
  padding: 7px 18px; background: var(--primary); border: none;
  border-radius: var(--rad); color: #fff; cursor: pointer; font-size: 12.5px; font-weight: 600; transition: all .15s;
}
.modal-confirm:hover { background: var(--primary-d); }

/* ── TOAST ────────────────────────────────────────────────── */
#toastWrap {
  position: fixed; bottom: 22px; right: 22px;
  z-index: 9000; display: flex; flex-direction: column; gap: 8px;
  pointer-events: none;
}
.toast {
  padding: 10px 16px; border-radius: 8px;
  font-size: 13px; font-weight: 500;
  box-shadow: 0 4px 20px rgba(0,0,0,.25);
  opacity: 0; transform: translateY(8px);
  transition: all .22s ease;
  pointer-events: auto; max-width: 340px;
}
.toast.show { opacity: 1; transform: translateY(0); }
.toast-info    { background: #1E2840; border: 1px solid rgba(79,126,247,.4); color: #93B4FC; }
.toast-success { background: #052e16; border: 1px solid #16A34A; color: #4ADE80; }
.toast-error   { background: #450a0a; border: 1px solid #DC2626; color: #FCA5A5; }
.toast-warn    { background: #431407; border: 1px solid #D97706; color: #FCD34D; }

/* ── CUSTOM CONFIRM ───────────────────────────────────────── */
#confirmModal .modal-box { width: 340px; }
#confirmModal .confirm-msg { font-size: 13.5px; color: rgba(255,255,255,.8); line-height: 1.6; margin-bottom: 18px; }

/* ── CURL BADGE ───────────────────────────────────────────── */
.curl-badge {
  display: none;
  align-items: center; gap: 5px;
  font-size: 11px; font-weight: 600; color: #4ADE80;
  background: rgba(22,163,74,.15); border: 1px solid rgba(22,163,74,.3);
  padding: 3px 9px; border-radius: 100px;
}
.curl-badge.show { display: flex; }

/* ── MISC ─────────────────────────────────────────────────── */
.hidden { display: none !important; }

@media (max-width: 750px) {
  .sidebar { display: none; }
  .stats-grid { grid-template-columns: repeat(2,1fr); }
  .runner-grid { grid-template-columns: 1fr; }
  .body-grid   { grid-template-columns: 1fr; }
}
</style>
</head>
<body>

<!-- ═══════════════════════════════════════════════════════ -->
<!-- HEADER                                                  -->
<!-- ═══════════════════════════════════════════════════════ -->
<header class="app-header">
  <div class="app-logo">
    <svg width="34" height="34" viewBox="0 0 34 34" fill="none">
      <defs>
        <linearGradient id="hg" x1="0" y1="0" x2="34" y2="34" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stop-color="#2563eb"/>
          <stop offset="100%" stop-color="#7c3aed"/>
        </linearGradient>
        <linearGradient id="hbolt" x1="10" y1="4" x2="24" y2="30" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stop-color="#ffffff"/>
          <stop offset="100%" stop-color="#bfdbfe"/>
        </linearGradient>
      </defs>
      <rect width="34" height="34" rx="9" fill="url(#hg)"/>
      <rect width="34" height="34" rx="9" fill="white" opacity="0.06"/>
      <path d="M21,5 L10,19 L18,19 L13,29 L24,15 L16,15 Z" fill="url(#hbolt)"/>
    </svg>
    <div class="app-name-group">
      <span class="app-name">API Runner</span>
      <span class="app-sub">CSV · JSON fayldan API ga batch so'rov</span>
    </div>
  </div>
  <span class="app-ver">v2</span>
  <div class="hdr-space"></div>
  <div class="hdr-chips">
    <span class="hdr-chip">
      <svg width="11" height="11" viewBox="0 0 11 11" fill="none">
        <path d="M1.5 2.5h8M1.5 5.5h5M1.5 8.5h6.5" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/>
      </svg>
      Postman Runner alternativ
    </span>
    <span class="hdr-chip">
      <svg width="11" height="11" viewBox="0 0 11 11" fill="none">
        <circle cx="5.5" cy="5.5" r="4.5" stroke="currentColor" stroke-width="1.3"/>
        <path d="M5.5 3.5v2.2l1.3 1.3" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>
      </svg>
      Real-time natija
    </span>
  </div>
</header>

<!-- ═══════════════════════════════════════════════════════ -->
<!-- MODALS                                                  -->
<!-- ═══════════════════════════════════════════════════════ -->

<!-- Toast container -->
<div id="toastWrap"></div>

<!-- Custom confirm -->
<div class="modal-overlay hidden" id="confirmModal">
  <div class="modal-box">
    <div class="modal-title">Tasdiqlash</div>
    <div class="confirm-msg" id="confirmMsg"></div>
    <div class="modal-btns">
      <button class="modal-cancel" id="confirmCancelBtn">Bekor</button>
      <button class="modal-confirm" style="background:#DC2626" id="confirmOkBtn">O'chirish</button>
    </div>
  </div>
</div>

<!-- Save modal -->
<div class="modal-overlay hidden" id="saveModal">
  <div class="modal-box">
    <div class="modal-title">So'rovni saqlash</div>
    <div class="modal-field">
      <div class="modal-label">Nom</div>
      <input id="saveName" class="modal-input" placeholder="Get User, Create Order, Login..." maxlength="80">
    </div>
    <div class="modal-field">
      <div class="modal-label">Papka (ixtiyoriy)</div>
      <select id="saveFolderSel" class="modal-select">
        <option value="">— Papkasiz —</option>
      </select>
    </div>
    <div class="modal-btns">
      <button class="modal-cancel" onclick="closeSaveModal()">Bekor</button>
      <button class="modal-confirm" onclick="confirmSave()">Saqlash</button>
    </div>
  </div>
</div>

<!-- Folder create/rename modal -->
<div class="modal-overlay hidden" id="folderModal">
  <div class="modal-box">
    <div class="modal-title" id="folderModalTitle">Yangi papka</div>
    <div class="modal-field">
      <div class="modal-label">Papka nomi</div>
      <input id="folderName" class="modal-input" placeholder="User API, Auth, Orders..." maxlength="60">
    </div>
    <div class="modal-btns">
      <button class="modal-cancel" onclick="closeFolderModal()">Bekor</button>
      <button class="modal-confirm" onclick="confirmFolder()">Saqlash</button>
    </div>
  </div>
</div>

<!-- Move modal -->
<div class="modal-overlay hidden" id="moveModal">
  <div class="modal-box">
    <div class="modal-title">Papkaga ko'chirish</div>
    <div class="modal-field">
      <div class="modal-label">Papkani tanlang</div>
      <select id="moveFolderSel" class="modal-select">
        <option value="">— Papkasiz (root) —</option>
      </select>
    </div>
    <div class="modal-btns">
      <button class="modal-cancel" onclick="closeMoveModal()">Bekor</button>
      <button class="modal-confirm" onclick="confirmMove()">Ko'chirish</button>
    </div>
  </div>
</div>

<!-- ═══════════════════════════════════════════════════════ -->
<!-- LAYOUT                                                  -->
<!-- ═══════════════════════════════════════════════════════ -->
<div class="layout">

<!-- ─── SIDEBAR ─────────────────────────────────────────── -->
<aside class="sidebar">

  <!-- Top buttons -->
  <div class="sb-actions">
    <button class="sb-btn sb-btn-req" onclick="newRequest()">
      <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
        <path d="M6 1v10M1 6h10" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
      </svg>
      Yangi so'rov
    </button>
    <button class="sb-btn sb-btn-folder" onclick="openFolderModal(null)">
      <svg width="13" height="12" viewBox="0 0 13 12" fill="none">
        <path d="M1 3a1 1 0 011-1h3l1.5 1.5H11a1 1 0 011 1V10a1 1 0 01-1 1H2a1 1 0 01-1-1V3z" stroke="currentColor" stroke-width="1.3" stroke-linejoin="round"/>
        <path d="M6.5 5.5v3M5 7h3" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>
      </svg>
      Papka
    </button>
  </div>

  <!-- Search -->
  <div class="sb-search-wrap">
    <svg class="sb-search-ico" width="12" height="12" viewBox="0 0 12 12" fill="none">
      <circle cx="5" cy="5" r="3.5" stroke="currentColor" stroke-width="1.3"/>
      <path d="M8 8l2.5 2.5" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>
    </svg>
    <input class="sb-search" id="sbSearch" placeholder="Qidirish..." oninput="filterSidebar(this.value)">
  </div>

  <!-- Tree -->
  <div class="sb-tree" id="sbTree">
    <div class="sb-empty">
      <div class="sb-empty-ico">📂</div>
      <div>Hali so'rovlar yo'q.<br>So'rov yaratib saqlang.</div>
    </div>
  </div>

</aside>

<div class="sb-resizer" id="sbResizer">
  <button class="sb-resizer-toggle" id="sbResizerToggle" onclick="toggleSidebar()" title="Sidebar yashirish / ko'rsatish">◀</button>
</div>

<!-- ─── CONTENT AREA ────────────────────────────────────── -->
<div class="content-area">
<div class="main-scroll">

  <!-- ── CARD: REQUEST ──────────────────────────────────── -->
  <div class="card">
    <div class="card-header">
      <span class="card-title">So'rov</span>
      <div class="card-acts">
        <button class="btn-curl" id="btnCurl" onclick="copyCurl()" title="CURL ko'chirish">
          <svg width="13" height="13" viewBox="0 0 13 13" fill="none">
            <rect x="1" y="4" width="7" height="8" rx="1.5" stroke="currentColor" stroke-width="1.3"/>
            <path d="M4 4V2.5A1.5 1.5 0 015.5 1h5A1.5 1.5 0 0112 2.5v8A1.5 1.5 0 0110.5 12H9" stroke="currentColor" stroke-width="1.3"/>
            <path d="M3.5 9l1.5-1.5L3.5 6" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
          cURL
        </button>
        <button class="btn-save" id="btnSave" onclick="openSaveModal()" disabled>
          <svg width="13" height="13" viewBox="0 0 13 13" fill="none">
            <path d="M2 1h7l2 2v9H2V1z" stroke="white" stroke-width="1.3" stroke-linejoin="round"/>
            <rect x="4" y="8" width="5" height="4" rx=".5" stroke="white" stroke-width="1.2"/>
            <rect x="3.5" y="1" width="5" height="3" rx=".5" fill="white" opacity=".5"/>
          </svg>
          Saqlash
        </button>
      </div>
    </div>
    <div class="card-body">

      <!-- URL BAR -->
      <div class="url-bar">
        <select id="method" class="method-sel">
          <option>GET</option>
          <option selected>POST</option>
          <option>PUT</option>
          <option>PATCH</option>
          <option>DELETE</option>
        </select>
        <input id="url" class="url-input" placeholder="https://api.example.com/endpoint/{{id}}  yoki  curl '...' ni bu yerga joylashtiring">
        <span class="curl-badge" id="curlBadge">
          <svg width="11" height="11" viewBox="0 0 11 11" fill="none">
            <path d="M1 5.5L4 8.5L10 2.5" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
          CURL
        </span>
        <button id="runBtn" class="btn btn-primary url-run-btn" onclick="startRun()">▶ RUN</button>
        <button id="stopBtn" class="btn btn-danger url-run-btn" onclick="stopRun()" disabled>■ STOP</button>
      </div>

      <!-- TABS -->
      <div class="tabs">
        <button class="tab-btn active" data-tab="params">Params</button>
        <button class="tab-btn" data-tab="headers">Headers</button>
        <button class="tab-btn" data-tab="auth">Auth</button>
        <button class="tab-btn" data-tab="body">Body</button>
      </div>

      <!-- TAB: PARAMS -->
      <div id="tab-params" class="tab-panel active">
        <table class="kv-table" id="paramsTable">
          <thead><tr>
            <th style="width:40%">Key</th><th>Value</th><th style="width:34px"></th>
          </tr></thead>
          <tbody></tbody>
        </table>
        <button class="btn-add-row" onclick="addKvRow('paramsTable')">+ Param qo'shish</button>
      </div>

      <!-- TAB: HEADERS -->
      <div id="tab-headers" class="tab-panel">
        <table class="kv-table" id="headersTable">
          <thead><tr>
            <th style="width:40%">Key</th><th>Value</th><th style="width:34px"></th>
          </tr></thead>
          <tbody></tbody>
        </table>
        <button class="btn-add-row" onclick="addKvRow('headersTable')">+ Header qo'shish</button>
      </div>

      <!-- TAB: AUTH -->
      <div id="tab-auth" class="tab-panel">
        <div class="field-label">Authorization header</div>
        <input id="authorization" class="text-input" placeholder="Bearer eyJhbGci... yoki Basic xxx">
        <div class="field-hint">Yozilgan qiymat to'g'ridan-to'g'ri Authorization headeriga qo'yiladi. {{token}} kabi o'zgaruvchilar ishlaydi.</div>
      </div>

      <!-- TAB: BODY -->
      <div id="tab-body" class="tab-panel">
        <div class="body-grid">
          <div>
            <div class="field-label">Body turi</div>
            <select id="bodyType" class="select-input">
              <option value="json">JSON</option>
              <option value="raw">Raw text</option>
              <option value="form">Form encoded</option>
            </select>
          </div>
          <div>
            <div class="field-label">Content-Type</div>
            <input id="contentType" class="text-input" value="application/json">
          </div>
        </div>
        <div class="body-wrap">
          <textarea id="body" class="body-textarea" placeholder='{
  "id": "{{id}}",
  "name": "{{name}}"
}'></textarea>
          <button class="btn-beautify" id="btnBeautify" onclick="beautifyBody()" title="JSON ni chiroyli formatlash">✦ Beautify</button>
        </div>
      </div>

    </div>
  </div>

  <!-- ── CARD: RUNNER ───────────────────────────────────── -->
  <div class="card">
    <div class="card-header">
      <span class="card-title">Runner — Iteration Data</span>
      <div class="card-acts" style="font-size:11.5px;color:var(--muted);">
        Fayl <b>ixtiyoriy</b> — faylsiz bitta so'rov yuboriladi
      </div>
    </div>
    <div class="card-body">

      <div class="upload-zone" id="uploadZone">
        <input type="file" id="dataFile" accept=".csv,.json" onchange="handleFile()">
        <div class="upload-ico">📂</div>
        <div class="upload-title">CSV yoki JSON fayl tanlang (ixtiyoriy)</div>
        <div class="upload-sub">Faylsiz — bitta so'rov &nbsp;·&nbsp; Fayl bilan — har qator uchun alohida so'rov &nbsp;·&nbsp; {{ustun}} o'zgaruvchilar ishlaydi</div>
      </div>

      <div id="fileBar" class="file-bar hidden">
        <span>📄</span>
        <span id="fileName" class="file-bar-name"></span>
        <span id="fileMeta" class="file-bar-meta"></span>
        <button class="file-bar-clear" onclick="clearFile()" title="Faylni olib tashlash">×</button>
      </div>

      <div id="previewWrap" class="preview-wrap hidden">
        <table class="preview-table" id="previewTable"></table>
        <div id="previewMore" class="preview-more hidden"></div>
      </div>

      <div class="runner-grid">
        <div>
          <div class="field-label">Qatorlar</div>
          <select id="rowsMode" class="select-input" onchange="toggleCount()">
            <option value="all">Hammasi</option>
            <option value="custom">Maxsus son</option>
          </select>
        </div>
        <div id="countWrap" class="hidden">
          <div class="field-label">Son</div>
          <input id="count" class="text-input" type="number" value="10" min="1">
        </div>
        <div>
          <div class="field-label">Delay (soniya)</div>
          <input id="delay" class="text-input" type="number" value="0" min="0" step="0.1" placeholder="0">
        </div>
      </div>

    </div>
  </div>

</div><!-- /.main-scroll -->

<!-- ─── RESIZER ──────────────────────────────────────────── -->
<div class="resizer" id="resizer">
  <button class="resizer-toggle" id="resizerToggle" onclick="toggleResults()" title="Natijalarni ko'rsatish / yopish">▶</button>
</div>

<!-- ─── RESULTS PANE ─────────────────────────────────────── -->
<div class="results-pane closed" id="resultsPane">
  <div class="rp-header">
    <span class="rp-title">Natijalar</span>
    <div class="status-bar" id="statusBar" style="margin:0;border:none;background:none;padding:0 8px 0 12px;flex:1;">
      <div class="status-dot" id="statusDot"></div>
      <span id="statusTxt" style="font-size:12px;">Tayyor</span>
    </div>
    <span class="rp-spacer"></span>
    <button class="btn btn-ghost btn-sm" onclick="exportJSON()">⬇ JSON</button>
    <button class="btn btn-ghost btn-sm" onclick="exportCSV()">⬇ CSV</button>
    <button class="rp-close" onclick="closeResults()" title="Yopish">×</button>
  </div>
  <div class="rp-body">
    <div class="prog-head">
      <span class="prog-label">Progress</span>
      <span class="prog-count" id="progCount">0 / 0</span>
    </div>
    <div class="prog-bar" style="margin-bottom:14px;"><div class="prog-fill" id="progFill"></div></div>

    <div class="stats-grid">
      <div class="stat-card"><div class="stat-val" id="sTotal">0</div><div class="stat-lbl">Jami</div></div>
      <div class="stat-card s-ok"><div class="stat-val" id="sOk">0</div><div class="stat-lbl">Muvaffaqiyatli</div></div>
      <div class="stat-card s-err"><div class="stat-val" id="sErr">0</div><div class="stat-lbl">Xatolik</div></div>
      <div class="stat-card s-time"><div class="stat-val" id="sAvg">—</div><div class="stat-lbl">O'rtacha vaqt</div></div>
    </div>

    <div class="filter-bar">
      <button class="flt-btn active" id="f-all"     onclick="setFilter('all')">Hammasi</button>
      <button class="flt-btn f-ok"   id="f-success" onclick="setFilter('success')">✓ Muvaffaqiyatli</button>
      <button class="flt-btn f-err"  id="f-error"   onclick="setFilter('error')">✕ Xatolik</button>
    </div>

    <div id="resultsList">
      <div class="empty-state">
        <div class="empty-ico">📭</div>
        <div>Natijalar bu yerda ko'rinadi</div>
      </div>
    </div>
  </div>
</div><!-- /.results-pane -->

</div><!-- /.content-area -->
</div><!-- /.layout -->


<script>
// ════════════════════════════════════════════════════════════
// STATE
// ════════════════════════════════════════════════════════════
let dataRows     = [];
let allResults   = [];
let currentFilter = 'all';
let currentJobId  = null;
let currentSrc    = null;

// sidebar state
let db          = { folders: [], requests: [] };
let activeReqId = null;   // currently loaded request id
let saveEditId  = null;   // for updating existing saved request
let folderEditId = null;  // for renaming folder
let moveReqId   = null;   // request being moved
let searchQuery = '';


// ════════════════════════════════════════════════════════════
// TABS
// ════════════════════════════════════════════════════════════
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    const tab = btn.dataset.tab;
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('tab-' + tab).classList.add('active');
  });
});

// ── BODY TEXTAREA: Tab key inserts spaces, not focus-jump ──
const bodyTA = document.getElementById('body');
bodyTA.addEventListener('keydown', e => {
  if (e.key === 'Tab') {
    e.preventDefault();
    const s = bodyTA.selectionStart, end = bodyTA.selectionEnd;
    bodyTA.value = bodyTA.value.slice(0, s) + '  ' + bodyTA.value.slice(end);
    bodyTA.selectionStart = bodyTA.selectionEnd = s + 2;
  }
});

// ── BEAUTIFY button: show only when bodyType === json ──
const btnBeautify = document.getElementById('btnBeautify');
function updateBeautifyVisibility() {
  const t = document.getElementById('bodyType').value;
  btnBeautify.classList.toggle('visible', t === 'json');
}
document.getElementById('bodyType').addEventListener('change', updateBeautifyVisibility);
updateBeautifyVisibility();

function beautifyBody() {
  try {
    const raw = bodyTA.value.trim();
    if (!raw) return;
    bodyTA.value = JSON.stringify(JSON.parse(raw), null, 2);
    bodyTA.style.transition = 'box-shadow .2s';
    bodyTA.style.boxShadow = '0 0 0 3px rgba(79,126,247,.2)';
    setTimeout(() => { bodyTA.style.boxShadow = ''; }, 600);
  } catch {
    showToast('JSON formati noto\'g\'ri', 'error');
  }
}


// ════════════════════════════════════════════════════════════
// KV TABLE
// ════════════════════════════════════════════════════════════
function addKvRow(tid, name='', value='') {
  const tbody = document.querySelector('#' + tid + ' tbody');
  const tr = document.createElement('tr');
  tr.innerHTML = `
    <td><input class="kv-name" value="${ea(name)}" placeholder="key"></td>
    <td><input class="kv-val"  value="${ea(value)}" placeholder="{{column}}"></td>
    <td class="kv-del" onclick="this.closest('tr').remove()">×</td>`;
  tbody.appendChild(tr);
}

function getKv(tid) {
  return [...document.querySelectorAll('#' + tid + ' tbody tr')]
    .map(tr => ({ name: tr.querySelector('.kv-name').value.trim(), value: tr.querySelector('.kv-val').value }))
    .filter(r => r.name);
}

addKvRow('paramsTable');
addKvRow('headersTable');


// ════════════════════════════════════════════════════════════
// FILE UPLOAD
// ════════════════════════════════════════════════════════════
const uploadZone = document.getElementById('uploadZone');
uploadZone.addEventListener('dragover', e => { e.preventDefault(); uploadZone.classList.add('drag-over'); });
uploadZone.addEventListener('dragleave', () => uploadZone.classList.remove('drag-over'));
uploadZone.addEventListener('drop', e => {
  e.preventDefault(); uploadZone.classList.remove('drag-over');
  const f = e.dataTransfer.files[0]; if (f) processFile(f);
});

function handleFile() {
  const f = document.getElementById('dataFile').files[0]; if (f) processFile(f);
}

function processFile(file) {
  const reader = new FileReader();
  reader.onload = ev => {
    try {
      const text = ev.target.result;
      if (file.name.toLowerCase().endsWith('.json')) {
        const parsed = JSON.parse(text);
        if (!Array.isArray(parsed)) throw new Error("JSON array bo'lishi kerak");
        dataRows = parsed;
      } else {
        dataRows = parseCSV(text);
      }
      if (!dataRows.length) throw new Error("Faylda ma'lumot topilmadi");
      document.getElementById('fileName').textContent = file.name;
      document.getElementById('fileMeta').textContent =
        dataRows.length + ' qator · ' + Object.keys(dataRows[0]).length + ' ustun';
      document.getElementById('fileBar').classList.remove('hidden');
      uploadZone.classList.add('hidden');
      showPreview(dataRows);
    } catch(err) {
      showToast('Fayl xatosi: ' + err.message, 'error');
      clearFile();
    }
  };
  reader.readAsText(file);
}

function clearFile() {
  dataRows = [];
  document.getElementById('dataFile').value = '';
  document.getElementById('fileBar').classList.add('hidden');
  uploadZone.classList.remove('hidden');
  document.getElementById('previewWrap').classList.add('hidden');
}

function showPreview(rows) {
  const cols = Object.keys(rows[0]);
  const preview = rows.slice(0, 5);
  let html = '<thead><tr>' + cols.map(c => `<th>${eh(c)}</th>`).join('') + '</tr></thead><tbody>';
  preview.forEach(row => {
    html += '<tr>' + cols.map(c => `<td title="${ea(String(row[c]??''))}">${eh(String(row[c]??''))}</td>`).join('') + '</tr>';
  });
  html += '</tbody>';
  document.getElementById('previewTable').innerHTML = html;
  document.getElementById('previewWrap').classList.remove('hidden');
  const more = document.getElementById('previewMore');
  if (rows.length > 5) { more.textContent = `... va yana ${rows.length - 5} ta qator`; more.classList.remove('hidden'); }
  else more.classList.add('hidden');
}


// ════════════════════════════════════════════════════════════
// CSV PARSER
// ════════════════════════════════════════════════════════════
function parseCSV(text) {
  const lines = text.replace(/\r\n/g,'\n').replace(/\r/g,'\n').split('\n').filter(l => l.trim());
  if (lines.length < 2) return [];
  const delim = [',',';','\t'].reduce((best, d) => {
    const n = lines[0].split(d).length - 1;
    return n > lines[0].split(best).length - 1 ? d : best;
  }, ',');
  const headers = parseLine(lines[0], delim);
  return lines.slice(1).map(line => {
    const vals = parseLine(line, delim);
    const row = {};
    headers.forEach((h, i) => { row[h.trim()] = vals[i] ?? ''; });
    return row;
  });
}

function parseLine(line, delim) {
  const res = []; let cur = '', inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') { if (inQ && line[i+1] === '"') { cur += '"'; i++; } else inQ = !inQ; }
    else if (c === delim && !inQ) { res.push(cur); cur = ''; }
    else cur += c;
  }
  res.push(cur);
  return res;
}


// ════════════════════════════════════════════════════════════
// RUN
// ════════════════════════════════════════════════════════════
function toggleCount() {
  const m = document.getElementById('rowsMode').value;
  document.getElementById('countWrap').classList.toggle('hidden', m !== 'custom');
}

async function startRun() {
  const url = document.getElementById('url').value.trim();
  if (!url) { showToast('URL kiriting!', 'warn'); return; }

  let rows = dataRows.length ? dataRows : [{}];  // faylsiz — bitta bo'sh qator
  if (document.getElementById('rowsMode').value === 'custom') {
    const n = parseInt(document.getElementById('count').value) || 1;
    rows = dataRows.slice(0, n);
  }

  const config = {
    method: document.getElementById('method').value,
    url,
    authorization: document.getElementById('authorization').value.trim(),
    params:   getKv('paramsTable'),
    headers:  getKv('headersTable'),
    body:     document.getElementById('body').value,
    content_type: document.getElementById('contentType').value.trim(),
    body_type: document.getElementById('bodyType').value,
    delay: parseFloat(document.getElementById('delay').value) || 0,
    rows,
  };

  allResults = [];
  document.getElementById('resultsList').innerHTML = '';
  openResults();
  setStatus('running', `${rows.length} ta so'rov yuborilmoqda...`);
  updateProgress(0, rows.length);
  updateStats(0, 0, 0, null);
  setFilter('all');
  document.getElementById('runBtn').disabled  = true;
  document.getElementById('stopBtn').disabled = false;

  let jobId;
  try {
    const r = await fetch('/run', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(config),
    });
    const d = await r.json();
    if (!r.ok) throw new Error(d.error || 'Server xatosi');
    jobId = d.job_id;
  } catch(err) {
    setStatus('error', 'Xatolik: ' + err.message);
    showToast('Xatolik: ' + err.message, 'error');
    document.getElementById('runBtn').disabled  = false;
    document.getElementById('stopBtn').disabled = true;
    return;
  }

  currentJobId = jobId;
  if (currentSrc) currentSrc.close();
  const src = new EventSource('/stream/' + jobId);
  currentSrc = src;

  src.addEventListener('result', ev => {
    const item = JSON.parse(ev.data);
    allResults.push(item);
    renderResult(item);
    updateProgress(item.index, item.total);
    updateStats(item.total, item.successful, item.failed, null);
    setStatus('running', `${item.index} / ${item.total}`);
  });

  src.addEventListener('done', ev => {
    const s = JSON.parse(ev.data);
    src.close(); currentSrc = null; currentJobId = null;
    const fill = document.getElementById('progFill');
    fill.classList.remove('done','stopped');
    fill.classList.add(s.stopped ? 'stopped' : 'done');
    updateProgress(s.completed, s.total);
    updateStats(s.total, s.successful, s.failed, s.avg_time);
    setStatus(s.stopped ? 'stopped' : 'done',
      s.stopped ? `To'xtatildi — ${s.completed} ta yuborildi`
                : `Bajarildi — ${s.successful} muvaffaqiyatli, ${s.failed} xatolik`);
    document.getElementById('runBtn').disabled  = false;
    document.getElementById('stopBtn').disabled = true;
  });

  src.onerror = () => { if (currentSrc) { src.close(); currentSrc = null; } };
}

async function stopRun() {
  if (!currentJobId) return;
  await fetch('/stop/' + currentJobId, { method: 'POST' });
  document.getElementById('stopBtn').disabled = true;
}


// ════════════════════════════════════════════════════════════
// UI HELPERS
// ════════════════════════════════════════════════════════════
function setStatus(state, text) {
  document.getElementById('statusDot').className = 'status-dot ' + state;
  document.getElementById('statusTxt').textContent = text;
}

function updateProgress(cur, total) {
  const pct = total > 0 ? (cur / total * 100) : 0;
  document.getElementById('progFill').style.width = pct + '%';
  document.getElementById('progCount').textContent = cur + ' / ' + total;
}

function updateStats(total, ok, err, avg) {
  document.getElementById('sTotal').textContent = total;
  document.getElementById('sOk').textContent    = ok;
  document.getElementById('sErr').textContent   = err;
  document.getElementById('sAvg').textContent   = avg != null ? avg.toFixed(3) + 's' : '—';
}

function badgeClass(status) {
  if (typeof status === 'number') {
    if (status >= 200 && status < 300) return 'b-2xx';
    if (status >= 400 && status < 500) return 'b-4xx';
    if (status >= 500) return 'b-5xx';
  }
  return 'b-err';
}

function renderResult(item) {
  const list = document.getElementById('resultsList');
  const em = list.querySelector('.empty-state');
  if (em) em.remove();

  const ok = typeof item.status === 'number' && item.status >= 200 && item.status < 300;
  if (currentFilter === 'success' && !ok) return;
  if (currentFilter === 'error'   &&  ok) return;

  const respStr = typeof item.response === 'object'
    ? JSON.stringify(item.response, null, 2) : String(item.response ?? '');

  const card = document.createElement('div');
  card.className = 'result-card ' + (ok ? 'is-ok' : 'is-err');
  card.dataset.ok = ok ? '1' : '0';
  card.innerHTML = `
    <div class="result-head" onclick="this.parentElement.classList.toggle('expanded')">
      <span class="result-num">#${item.index}</span>
      <span class="status-badge ${badgeClass(item.status)}">${eh(String(item.status))}</span>
      <span class="result-url" title="${ea(item.url)}">${eh(item.url)}</span>
      <span class="result-time">${item.time.toFixed(3)}s</span>
      <span class="result-sz">${fmtBytes(item.size)}</span>
      <span class="chevron">▼</span>
    </div>
    <div class="result-body">
      <div class="result-fl">URL</div>
      <div class="url-display">${eh(item.url)}</div>
      <div class="result-fl">Response</div>
      <div class="resp-wrap">
        <button class="copy-btn" onclick="copyResp(this)">Copy</button>
        <pre class="resp-pre">${syntaxHL(respStr)}</pre>
      </div>
    </div>`;
  list.appendChild(card);
}

function copyResp(btn) {
  navigator.clipboard.writeText(btn.nextElementSibling.textContent).then(() => {
    btn.textContent = 'Copied!'; btn.classList.add('copied');
    setTimeout(() => { btn.textContent = 'Copy'; btn.classList.remove('copied'); }, 1500);
  });
}

function setFilter(f) {
  currentFilter = f;
  ['all','success','error'].forEach(id =>
    document.getElementById('f-' + id).classList.toggle('active', id === f));
  document.querySelectorAll('.result-card').forEach(card => {
    const ok = card.dataset.ok === '1';
    card.style.display =
      (f === 'success' && !ok) || (f === 'error' && ok) ? 'none' : '';
  });
}

function fmtBytes(n) {
  if (!n) return '0 B';
  if (n < 1024) return n + ' B';
  if (n < 1048576) return (n/1024).toFixed(1) + ' KB';
  return (n/1048576).toFixed(1) + ' MB';
}

function syntaxHL(str) {
  const s = str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  return s.replace(
    /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g,
    m => {
      if (/^"/.test(m)) return /:$/.test(m) ? `<span class="j-key">${m}</span>` : `<span class="j-str">${m}</span>`;
      if (/true|false/.test(m)) return `<span class="j-bool">${m}</span>`;
      if (/null/.test(m))       return `<span class="j-null">${m}</span>`;
      return `<span class="j-num">${m}</span>`;
    });
}


// ════════════════════════════════════════════════════════════
// EXPORT
// ════════════════════════════════════════════════════════════
function exportJSON() {
  if (!allResults.length) { showToast("Natijalar yo'q", 'warn'); return; }
  dl(JSON.stringify(allResults, null, 2), 'api-results.json', 'application/json');
}
function exportCSV() {
  if (!allResults.length) { showToast("Natijalar yo'q", 'warn'); return; }
  const cols = ['index','status','time','size','url','response','error'];
  const rows = allResults.map(r =>
    cols.map(c => {
      const v = c === 'response' && typeof r[c] === 'object' ? JSON.stringify(r[c]) : String(r[c]??'');
      return '"' + v.replace(/"/g,'""') + '"';
    }).join(','));
  dl(cols.join(',') + '\n' + rows.join('\n'), 'api-results.csv', 'text/csv');
}
function dl(content, name, type) {
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([content], {type}));
  a.download = name; a.click();
}


// ════════════════════════════════════════════════════════════
// SIDEBAR — TREE
// ════════════════════════════════════════════════════════════

async function loadDB() {
  try {
    const res = await fetch('/saved');
    db = await res.json();
  } catch(_) {
    db = { folders: [], requests: [] };
  }
  renderTree();
}

function renderTree() {
  const tree = document.getElementById('sbTree');
  const q = searchQuery.toLowerCase();

  const matchReq = r =>
    !q || r.name.toLowerCase().includes(q) ||
    (r.url || '').toLowerCase().includes(q) ||
    (r.method || '').toLowerCase().includes(q);

  let html = '';

  // Folders
  db.folders.forEach(folder => {
    const folderReqs = db.requests.filter(r => r.folder_id === folder.id);
    const vis = folderReqs.filter(matchReq);
    if (q && !vis.length) return;

    const isOpen = folder.expanded !== false;
    html += `
      <div class="sb-folder" data-fid="${ea(folder.id)}">
        <div class="sb-folder-head" onclick="toggleFolder('${ea(folder.id)}')">
          <span class="sb-chevron">${isOpen ? '▾' : '▸'}</span>
          <span class="sb-folder-ico">📁</span>
          <span class="sb-folder-name" id="fn-${ea(folder.id)}">${eh(folder.name)}</span>
          <span class="sb-folder-count">${folderReqs.length}</span>
          <div class="sb-folder-acts">
            <button class="sb-act" onclick="event.stopPropagation();openFolderModal('${ea(folder.id)}')" title="Nomini o'zgartirish">✎</button>
            <button class="sb-act del" onclick="event.stopPropagation();deleteFolder('${ea(folder.id)}')" title="O'chirish">×</button>
          </div>
        </div>
        ${isOpen ? `<div class="sb-folder-body" id="fb-${ea(folder.id)}">
          ${(q ? vis : folderReqs).map(r => reqItem(r)).join('')}
          ${!folderReqs.length ? `<div style="padding:6px 8px;font-size:11px;color:rgba(255,255,255,.2);">Bo'sh papka</div>` : ''}
        </div>` : ''}
      </div>`;
  });

  // Ungrouped
  const ungrouped = db.requests.filter(r => !r.folder_id).filter(matchReq);
  if (ungrouped.length) {
    if (db.folders.length) {
      html += `<div class="sb-divider">Papkasiz</div>`;
    }
    ungrouped.forEach(r => { html += reqItem(r); });
  }

  if (!html) {
    html = `<div class="sb-empty">
      <div class="sb-empty-ico">${q ? '🔍' : '📂'}</div>
      <div>${q ? 'Hech narsa topilmadi' : "Hali so'rovlar yo'q.<br>So'rov yaratib saqlang."}</div>
    </div>`;
  }

  tree.innerHTML = html;
}

function reqItem(r) {
  const isActive = r.id === activeReqId;
  const shortUrl = (r.url || '').replace(/^https?:\/\//, '');
  return `
    <div class="sb-item ${isActive ? 'active' : ''}" data-rid="${ea(r.id)}" onclick="loadRequest('${ea(r.id)}')">
      <span class="mpill mpill-${ea(r.method||'GET')}">${eh(r.method||'GET')}</span>
      <div class="sb-item-info">
        <div class="sb-item-name" id="rn-${ea(r.id)}">${eh(r.name)}</div>
        <div class="sb-item-url">${eh(shortUrl)}</div>
      </div>
      <div class="sb-item-acts">
        <button class="sb-act" onclick="event.stopPropagation();openMoveModal('${ea(r.id)}')" title="Papkaga ko'chirish">⇄</button>
        <button class="sb-act" onclick="event.stopPropagation();startRenameReq('${ea(r.id)}')" title="Nomi o'zgartirish">✎</button>
        <button class="sb-act del" onclick="event.stopPropagation();deleteReq('${ea(r.id)}')" title="O'chirish">×</button>
      </div>
    </div>`;
}

function filterSidebar(q) {
  searchQuery = q;
  renderTree();
}

function toggleFolder(fid) {
  const folder = db.folders.find(f => f.id === fid);
  if (!folder) return;
  folder.expanded = folder.expanded === false ? true : false;
  renderTree();
  // persist expanded state
  fetch('/saved/folder/' + fid, {
    method: 'PUT',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ name: folder.name, expanded: folder.expanded }),
  }).catch(()=>{});
}


// ════════════════════════════════════════════════════════════
// NEW REQUEST
// ════════════════════════════════════════════════════════════
function newRequest() {
  activeReqId = null;
  saveEditId  = null;
  document.getElementById('method').value        = 'POST';
  document.getElementById('url').value           = '';
  document.getElementById('authorization').value = '';
  document.getElementById('body').value          = '';
  document.getElementById('contentType').value   = 'application/json';
  document.getElementById('bodyType').value      = 'json';
  document.querySelector('#paramsTable tbody').innerHTML  = '';
  document.querySelector('#headersTable tbody').innerHTML = '';
  addKvRow('paramsTable');
  addKvRow('headersTable');
  renderTree();
  _savedSnapshot = null;
  btnSave.disabled = true;
  // focus URL
  document.getElementById('url').focus();
}


// ════════════════════════════════════════════════════════════
// LOAD REQUEST
// ════════════════════════════════════════════════════════════
function loadRequest(rid) {
  const r = db.requests.find(x => x.id === rid);
  if (!r) return;
  activeReqId = rid;
  saveEditId  = rid;

  document.getElementById('method').value        = r.method        || 'GET';
  document.getElementById('url').value           = r.url           || '';
  document.getElementById('authorization').value = r.authorization || '';
  document.getElementById('body').value          = r.body          || '';
  document.getElementById('contentType').value   = r.content_type  || 'application/json';
  document.getElementById('bodyType').value      = r.body_type     || 'json';

  document.querySelector('#paramsTable tbody').innerHTML  = '';
  document.querySelector('#headersTable tbody').innerHTML = '';
  (r.params  || []).forEach(x => addKvRow('paramsTable',  x.name, x.value));
  (r.headers || []).forEach(x => addKvRow('headersTable', x.name, x.value));
  if (!(r.params  || []).length) addKvRow('paramsTable');
  if (!(r.headers || []).length) addKvRow('headersTable');

  renderTree();
  markSaved();
}


// ════════════════════════════════════════════════════════════
// SAVE MODAL
// ════════════════════════════════════════════════════════════
function openSaveModal() {
  // populate folder select
  const sel = document.getElementById('saveFolderSel');
  sel.innerHTML = '<option value="">— Papkasiz —</option>' +
    db.folders.map(f => `<option value="${ea(f.id)}">${eh(f.name)}</option>`).join('');

  // pre-fill name
  const inp = document.getElementById('saveName');
  if (saveEditId) {
    const existing = db.requests.find(r => r.id === saveEditId);
    inp.value = existing ? existing.name : '';
    if (existing && existing.folder_id) sel.value = existing.folder_id;
  } else {
    const url  = document.getElementById('url').value.trim();
    const meth = document.getElementById('method').value;
    const frag = url.split('/').filter(Boolean).slice(-2).join(' / ') || url;
    inp.value = frag ? meth + ' — ' + frag : '';
    sel.value = '';
  }

  document.getElementById('saveModal').classList.remove('hidden');
  setTimeout(() => inp.focus(), 60);
  inp.onkeydown = e => { if (e.key === 'Enter') confirmSave(); if (e.key === 'Escape') closeSaveModal(); };
}

function closeSaveModal() {
  document.getElementById('saveModal').classList.add('hidden');
}

async function confirmSave() {
  const name = document.getElementById('saveName').value.trim();
  if (!name) { document.getElementById('saveName').focus(); return; }
  const folderId = document.getElementById('saveFolderSel').value || null;

  const payload = {
    name, folder_id: folderId,
    method:        document.getElementById('method').value,
    url:           document.getElementById('url').value,
    authorization: document.getElementById('authorization').value,
    params:        getKv('paramsTable'),
    headers:       getKv('headersTable'),
    body:          document.getElementById('body').value,
    content_type:  document.getElementById('contentType').value,
    body_type:     document.getElementById('bodyType').value,
  };
  if (saveEditId) payload.id = saveEditId;

  try {
    const res  = await fetch('/saved/request', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Xatolik');
    activeReqId = data.id;
    saveEditId  = data.id;
    closeSaveModal();
    await loadDB();
    markSaved();
    showToast('"' + name + '" saqlandi', 'success');
  } catch(err) {
    showToast('Saqlashda xatolik: ' + err.message, 'error');
  }
}


// ════════════════════════════════════════════════════════════
// FOLDER MODAL
// ════════════════════════════════════════════════════════════
function openFolderModal(fid) {
  folderEditId = fid;
  const inp = document.getElementById('folderName');
  document.getElementById('folderModalTitle').textContent =
    fid ? "Papka nomini o'zgartirish" : "Yangi papka";
  if (fid) {
    const f = db.folders.find(x => x.id === fid);
    inp.value = f ? f.name : '';
  } else {
    inp.value = '';
  }
  document.getElementById('folderModal').classList.remove('hidden');
  setTimeout(() => inp.focus(), 60);
  inp.onkeydown = e => { if (e.key === 'Enter') confirmFolder(); if (e.key === 'Escape') closeFolderModal(); };
}

function closeFolderModal() {
  document.getElementById('folderModal').classList.add('hidden');
  folderEditId = null;
}

async function confirmFolder() {
  const name = document.getElementById('folderName').value.trim();
  if (!name) { document.getElementById('folderName').focus(); return; }

  try {
    if (folderEditId) {
      await fetch('/saved/folder/' + folderEditId, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name }),
      });
    } else {
      await fetch('/saved/folder', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name }),
      });
    }
    closeFolderModal();
    await loadDB();
  } catch(err) {
    showToast('Xatolik: ' + err.message, 'error');
  }
}

async function deleteFolder(fid) {
  const folder = db.folders.find(f => f.id === fid);
  const name = folder ? folder.name : '';
  const reqs = db.requests.filter(r => r.folder_id === fid).length;
  const msg = reqs
    ? `"${name}" papkasini o'chirish.\nIchidagi ${reqs} ta so'rov papkasiz bo'lib qoladi.`
    : `"${name}" papkasini o'chirmoqchimisiz?`;
  showConfirm(msg, "O'chirish", async () => {
    await fetch('/saved/folder/' + fid, { method: 'DELETE' });
    await loadDB();
    showToast('"' + name + '" papkasi o\'chirildi', 'info');
  });
}


// ════════════════════════════════════════════════════════════
// MOVE MODAL
// ════════════════════════════════════════════════════════════
function openMoveModal(rid) {
  moveReqId = rid;
  const r = db.requests.find(x => x.id === rid);
  const sel = document.getElementById('moveFolderSel');
  sel.innerHTML = '<option value="">— Papkasiz (root) —</option>' +
    db.folders.map(f => `<option value="${ea(f.id)}" ${r && r.folder_id === f.id ? 'selected' : ''}>${eh(f.name)}</option>`).join('');
  document.getElementById('moveModal').classList.remove('hidden');
}

function closeMoveModal() {
  document.getElementById('moveModal').classList.add('hidden');
  moveReqId = null;
}

async function confirmMove() {
  if (!moveReqId) return;
  const folderId = document.getElementById('moveFolderSel').value || null;
  try {
    await fetch('/saved/request/' + moveReqId, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ folder_id: folderId }),
    });
    closeMoveModal();
    await loadDB();
    showToast("Ko'chirildi", 'success');
  } catch(err) {
    showToast('Xatolik: ' + err.message, 'error');
  }
}


// ════════════════════════════════════════════════════════════
// DELETE & RENAME (requests)
// ════════════════════════════════════════════════════════════
async function deleteReq(rid) {
  const r = db.requests.find(x => x.id === rid);
  const name = r ? r.name : rid;
  showConfirm(`"${name}" ni o'chirmoqchimisiz?`, "O'chirish", async () => {
    await fetch('/saved/request/' + rid, { method: 'DELETE' });
    if (activeReqId === rid) { activeReqId = null; saveEditId = null; }
    await loadDB();
    showToast('"' + name + '" o\'chirildi', 'info');
  });
}

function startRenameReq(rid) {
  const el = document.getElementById('rn-' + rid);
  if (!el) return;
  const old = el.textContent;
  const inp = document.createElement('input');
  inp.className = 'sb-rename-input';
  inp.value = old;
  el.replaceWith(inp);
  inp.focus(); inp.select();

  async function commit() {
    const val = inp.value.trim() || old;
    try {
      await fetch('/saved/request/' + rid, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: val }),
      });
      const r = db.requests.find(x => x.id === rid);
      if (r) r.name = val;
    } catch(_) {}
    renderTree();
  }
  inp.addEventListener('blur', commit);
  inp.addEventListener('keydown', e => {
    if (e.key === 'Enter')  { e.preventDefault(); inp.blur(); }
    if (e.key === 'Escape') { inp.value = old; inp.blur(); }
  });
}


// ════════════════════════════════════════════════════════════
// UTILS
// ════════════════════════════════════════════════════════════
function eh(v) {
  const d = document.createElement('div');
  d.textContent = v;
  return d.innerHTML;
}
function ea(v) {
  return String(v).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}


// ════════════════════════════════════════════════════════════
// TOAST
// ════════════════════════════════════════════════════════════
function showToast(msg, type = 'info', dur = 3000) {
  const wrap  = document.getElementById('toastWrap');
  const toast = document.createElement('div');
  toast.className = 'toast toast-' + type;
  toast.textContent = msg;
  wrap.appendChild(toast);
  requestAnimationFrame(() => requestAnimationFrame(() => toast.classList.add('show')));
  setTimeout(() => {
    toast.classList.remove('show');
    setTimeout(() => toast.remove(), 280);
  }, dur);
}


// ════════════════════════════════════════════════════════════
// CUSTOM CONFIRM
// ════════════════════════════════════════════════════════════
function showConfirm(msg, okLabel, onOk) {
  document.getElementById('confirmMsg').textContent = msg;
  const okBtn = document.getElementById('confirmOkBtn');
  const cancelBtn = document.getElementById('confirmCancelBtn');
  okBtn.textContent = okLabel || "O'chirish";
  document.getElementById('confirmModal').classList.remove('hidden');

  function close() {
    document.getElementById('confirmModal').classList.add('hidden');
    okBtn.onclick = null;
    cancelBtn.onclick = null;
  }
  okBtn.onclick = () => { close(); onOk(); };
  cancelBtn.onclick = close;
}


// ════════════════════════════════════════════════════════════
// CURL PARSER
// ════════════════════════════════════════════════════════════
function tokenizeCurl(str) {
  const tokens = []; let i = 0;
  while (i < str.length) {
    while (i < str.length && /\s/.test(str[i])) i++;
    if (i >= str.length) break;
    if (str[i] === "'") {
      i++; let t = '';
      while (i < str.length && str[i] !== "'") t += str[i++];
      i++; tokens.push(t);
    } else if (str[i] === '"') {
      i++; let t = '';
      while (i < str.length && str[i] !== '"') {
        if (str[i] === '\\' && i + 1 < str.length) {
          i++;
          const c = str[i];
          t += c === 'n' ? '\n' : c === 't' ? '\t' : c === '"' ? '"' : '\\' + c;
        } else t += str[i];
        i++;
      }
      i++; tokens.push(t);
    } else {
      let t = '';
      while (i < str.length && !/\s/.test(str[i])) t += str[i++];
      tokens.push(t);
    }
  }
  return tokens;
}

function parseCurl(raw) {
  // normalize line continuations and extra whitespace
  const text = raw.trim()
    .replace(/\\\r?\n/g, ' ')
    .replace(/\r?\n/g, ' ')
    .trim();

  if (!/^curl\b/i.test(text)) return null;

  const tokens = tokenizeCurl(text);
  const result = { method: null, url: null, headers: [], body: null, auth: null };

  let i = 1;
  while (i < tokens.length) {
    const t = tokens[i];

    if (t === '-X' || t === '--request') {
      result.method = (tokens[++i] || '').toUpperCase();

    } else if (t === '-H' || t === '--header') {
      const hdr = tokens[++i] || '';
      const ci = hdr.indexOf(':');
      if (ci > 0) result.headers.push({ name: hdr.slice(0, ci).trim(), value: hdr.slice(ci + 1).trim() });

    } else if (['-d','--data','--data-raw','--data-binary','--data-ascii','--data-urlencode'].includes(t)) {
      result.body = tokens[++i] || '';
      if (!result.method) result.method = 'POST';

    } else if (t === '-u' || t === '--user') {
      result.auth = 'Basic ' + btoa(tokens[++i] || '');

    } else if (t === '--url') {
      result.url = tokens[++i] || '';

    } else if (t === '-G' || t === '--get') {
      result.method = 'GET';

    } else if (/^https?:\/\//i.test(t) || (!t.startsWith('-') && !result.url && t !== 'curl')) {
      result.url = t;

    } else if (t.startsWith('-') && !['--compressed','--silent','-s','-v','--verbose',
        '-L','--location','--insecure','-k','--no-keepalive','-i','--include',
        '--http1.1','--http2','-f','--fail','-o','--output'].includes(t)) {
      // skip unknown flags that take a value
      if (i + 1 < tokens.length && !tokens[i+1].startsWith('-') && !/^https?:\/\//i.test(tokens[i+1])) {
        i++;
      }
    }
    i++;
  }

  return result.url ? result : null;
}

function applyCurl(parsed) {
  if (!parsed) return false;

  // Split URL and query params
  let baseUrl = parsed.url;
  const qIdx = parsed.url.indexOf('?');
  if (qIdx >= 0) {
    baseUrl = parsed.url.slice(0, qIdx);
    const qs = parsed.url.slice(qIdx + 1);
    document.querySelector('#paramsTable tbody').innerHTML = '';
    new URLSearchParams(qs).forEach((v, k) => addKvRow('paramsTable', k, v));
    if (!document.querySelector('#paramsTable tbody tr')) addKvRow('paramsTable');
  } else {
    document.querySelector('#paramsTable tbody').innerHTML = '';
    addKvRow('paramsTable');
  }

  document.getElementById('url').value    = baseUrl;
  document.getElementById('method').value = parsed.method || 'GET';

  // Headers → split out Authorization and Content-Type
  document.querySelector('#headersTable tbody').innerHTML = '';
  let hasAuth = false;
  (parsed.headers || []).forEach(h => {
    const low = h.name.toLowerCase();
    if (low === 'authorization') {
      document.getElementById('authorization').value = h.value;
      hasAuth = true;
    } else if (low === 'content-type') {
      document.getElementById('contentType').value = h.value;
    } else {
      addKvRow('headersTable', h.name, h.value);
    }
  });
  if (!document.querySelector('#headersTable tbody tr')) addKvRow('headersTable');

  if (parsed.auth && !hasAuth) {
    document.getElementById('authorization').value = parsed.auth;
  }

  // Body
  if (parsed.body != null) {
    document.getElementById('body').value = parsed.body;
    try {
      JSON.parse(parsed.body);
      document.getElementById('bodyType').value    = 'json';
      document.getElementById('contentType').value = 'application/json';
    } catch(_) {
      document.getElementById('bodyType').value = 'raw';
    }
    // switch to Body tab
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    document.querySelector('[data-tab="body"]').classList.add('active');
    document.getElementById('tab-body').classList.add('active');
  }

  return true;
}


// ════════════════════════════════════════════════════════════
// INIT
// ════════════════════════════════════════════════════════════
loadDB();

// ════════════════════════════════════════════════════════════
// SIDEBAR TOGGLE + RESIZE
// ════════════════════════════════════════════════════════════
const sidebar         = document.querySelector('.sidebar');
const sbResizer       = document.getElementById('sbResizer');
const sbResizerToggle = document.getElementById('sbResizerToggle');

function toggleSidebar() {
  const closed = sidebar.classList.toggle('closed');
  sbResizerToggle.textContent = closed ? '▶' : '◀';
  sbResizerToggle.title = closed ? "Sidebar ko'rsatish" : "Sidebar yashirish";
}

let _sbDragging = false, _sbStartX = 0, _sbStartW = 0;
sbResizer.addEventListener('mousedown', e => {
  if (e.target === sbResizerToggle) return;
  if (sidebar.classList.contains('closed')) return;
  _sbDragging = true;
  _sbStartX   = e.clientX;
  _sbStartW   = sidebar.offsetWidth;
  sbResizer.classList.add('dragging');
  document.body.style.cursor     = 'col-resize';
  document.body.style.userSelect = 'none';
  e.preventDefault();
});
document.addEventListener('mousemove', e => {
  if (!_sbDragging) return;
  const delta = e.clientX - _sbStartX;
  const newW  = Math.max(180, Math.min(_sbStartW + delta, 420));
  sidebar.style.transition = 'none';
  sidebar.style.width = newW + 'px';
});
document.addEventListener('mouseup', () => {
  if (!_sbDragging) return;
  _sbDragging = false;
  sbResizer.classList.remove('dragging');
  document.body.style.cursor     = '';
  document.body.style.userSelect = '';
  sidebar.style.transition = '';
});

// ════════════════════════════════════════════════════════════
// COPY AS CURL
// ════════════════════════════════════════════════════════════
function buildCurl() {
  const method  = document.getElementById('method').value;
  const rawUrl  = document.getElementById('url').value.trim();
  const auth    = document.getElementById('authorization').value.trim();
  const body    = document.getElementById('body').value.trim();
  const ct      = document.getElementById('contentType').value.trim();
  const bt      = document.getElementById('bodyType').value;
  const params  = getKv('paramsTable').filter(p => p.name);
  const headers = getKv('headersTable').filter(h => h.name);

  if (!rawUrl) return null;

  // Build URL with query params
  let url = rawUrl;
  if (params.length) {
    const qs = params.map(p => encodeURIComponent(p.name) + '=' + encodeURIComponent(p.value)).join('&');
    url += (url.includes('?') ? '&' : '?') + qs;
  }

  const esc = s => s.replace(/'/g, "'\\''");

  let parts = [`curl -X ${method} '${esc(url)}'`];

  // Auth header
  if (auth) parts.push(`  -H 'Authorization: ${esc(auth)}'`);

  // Content-Type (only if body present)
  if (body && ct) parts.push(`  -H 'Content-Type: ${esc(ct)}'`);

  // Custom headers
  headers.forEach(h => parts.push(`  -H '${esc(h.name)}: ${esc(h.value)}'`));

  // Body
  if (body && ['POST','PUT','PATCH','DELETE'].includes(method)) {
    if (bt === 'json') {
      try { parts.push(`  -d '${esc(JSON.stringify(JSON.parse(body)))}'`); }
      catch { parts.push(`  -d '${esc(body)}'`); }
    } else {
      parts.push(`  -d '${esc(body)}'`);
    }
  }

  return parts.join(' \\\n');
}

function copyCurl() {
  const curl = buildCurl();
  if (!curl) { showToast('URL kiriting', 'error'); return; }
  navigator.clipboard.writeText(curl).then(() => {
    const btn = document.getElementById('btnCurl');
    btn.classList.add('copied');
    btn.querySelector('svg').style.display = 'none';
    const old = btn.innerHTML;
    btn.innerHTML = '✓ Nusxalandi';
    setTimeout(() => {
      btn.innerHTML = old;
      btn.querySelector('svg').style.display = '';
      btn.classList.remove('copied');
    }, 1800);
    showToast('cURL clipboard ga nusxalandi', 'success');
  }).catch(() => showToast('Nusxalashda xatolik', 'error'));
}

// ════════════════════════════════════════════════════════════
// SAVE BUTTON DIRTY TRACKING
// ════════════════════════════════════════════════════════════
const btnSave = document.getElementById('btnSave');
let _savedSnapshot = null;

function getFormSnapshot() {
  return JSON.stringify({
    method:    document.getElementById('method').value,
    url:       document.getElementById('url').value,
    auth:      document.getElementById('authorization').value,
    body:      document.getElementById('body').value,
    ct:        document.getElementById('contentType').value,
    bt:        document.getElementById('bodyType').value,
    params:    getKv('paramsTable'),
    headers:   getKv('headersTable'),
  });
}

function markSaved() {
  _savedSnapshot = getFormSnapshot();
  btnSave.disabled = true;
}

function checkDirty() {
  btnSave.disabled = (getFormSnapshot() === _savedSnapshot);
}

// Watch all relevant inputs
['method','url','authorization','body','contentType','bodyType'].forEach(id => {
  document.getElementById(id).addEventListener('input',  checkDirty);
  document.getElementById(id).addEventListener('change', checkDirty);
});
// Watch KV tables (params & headers)
['paramsTable','headersTable'].forEach(tid => {
  document.getElementById(tid).addEventListener('input',  checkDirty);
  document.getElementById(tid).addEventListener('click',  () => setTimeout(checkDirty, 50));
});

// ════════════════════════════════════════════════════════════
// RESULTS PANE TOGGLE + RESIZE
// ════════════════════════════════════════════════════════════
const resultsPane   = document.getElementById('resultsPane');
const resizer       = document.getElementById('resizer');
const resizerToggle = document.getElementById('resizerToggle');

function openResults() {
  resultsPane.classList.remove('closed');
  resizerToggle.textContent = '▶';
  resizerToggle.title = 'Natijalarni yopish';
}

function closeResults() {
  resultsPane.classList.add('closed');
  resizerToggle.textContent = '◀';
  resizerToggle.title = "Natijalarni ko'rsatish";
}

function toggleResults() {
  if (resultsPane.classList.contains('closed')) openResults();
  else closeResults();
}

// Drag resize
let _dragging = false, _startX = 0, _startW = 0;

resizer.addEventListener('mousedown', e => {
  if (e.target === resizerToggle) return;
  if (resultsPane.classList.contains('closed')) return;
  _dragging = true;
  _startX   = e.clientX;
  _startW   = resultsPane.offsetWidth;
  resizer.classList.add('dragging');
  document.body.style.cursor      = 'col-resize';
  document.body.style.userSelect  = 'none';
  e.preventDefault();
});

document.addEventListener('mousemove', e => {
  if (!_dragging) return;
  const delta  = _startX - e.clientX;  // drag left → widen
  const newW   = Math.max(280, Math.min(_startW + delta, window.innerWidth * 0.72));
  resultsPane.style.transition = 'none';
  resultsPane.style.width = newW + 'px';
});

document.addEventListener('mouseup', () => {
  if (!_dragging) return;
  _dragging = false;
  resizer.classList.remove('dragging');
  document.body.style.cursor     = '';
  document.body.style.userSelect = '';
  resultsPane.style.transition   = '';
});

// URL paste — CURL detection
document.getElementById('url').addEventListener('paste', e => {
  const text = (e.clipboardData || window.clipboardData).getData('text');
  if (/^\s*curl\b/i.test(text)) {
    e.preventDefault();
    const parsed = parseCurl(text);
    if (parsed && applyCurl(parsed)) {
      const badge = document.getElementById('curlBadge');
      badge.classList.add('show');
      setTimeout(() => badge.classList.remove('show'), 3500);
      showToast('CURL muvaffaqiyatli tahlil qilindi', 'success');
    } else {
      showToast('CURL tahlil qilishda xatolik', 'error');
    }
  }
});
</script>
</body>
</html>"""


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def index():
    return HTML, 200, {"Content-Type": "text/html; charset=utf-8"}


# ── SAVED DATA ──────────────────────────────────────────────

@app.route("/saved")
def get_saved():
    return jsonify(load_db())


# Folder routes
@app.route("/saved/folder", methods=["POST"])
def create_folder():
    data = request.get_json() or {}
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "Nom kiritilmagan"}), 400
    db = load_db()
    folder = {
        "id": str(uuid.uuid4()),
        "name": name,
        "expanded": True,
        "created_at": now_iso(),
    }
    db["folders"].append(folder)
    save_db(db)
    return jsonify({"ok": True, "id": folder["id"]})


@app.route("/saved/folder/<fid>", methods=["PUT"])
def update_folder(fid):
    data = request.get_json() or {}
    db = load_db()
    for f in db["folders"]:
        if f["id"] == fid:
            if "name" in data:
                f["name"] = data["name"].strip() or f["name"]
            if "expanded" in data:
                f["expanded"] = bool(data["expanded"])
            f["updated_at"] = now_iso()
            break
    save_db(db)
    return jsonify({"ok": True})


@app.route("/saved/folder/<fid>", methods=["DELETE"])
def delete_folder(fid):
    db = load_db()
    db["folders"] = [f for f in db["folders"] if f["id"] != fid]
    # Move requests to root
    for r in db["requests"]:
        if r.get("folder_id") == fid:
            r["folder_id"] = None
    save_db(db)
    return jsonify({"ok": True})


# Request routes
@app.route("/saved/request", methods=["POST"])
def save_request():
    data = request.get_json() or {}
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "Nom kiritilmagan"}), 400

    db = load_db()
    rid = data.get("id")

    if rid:
        for i, r in enumerate(db["requests"]):
            if r["id"] == rid:
                data["updated_at"] = now_iso()
                data.setdefault("created_at", r.get("created_at", now_iso()))
                db["requests"][i] = data
                save_db(db)
                return jsonify({"ok": True, "id": rid})

    data["id"] = str(uuid.uuid4())
    data["created_at"] = now_iso()
    data.setdefault("folder_id", None)
    db["requests"].append(data)
    save_db(db)
    return jsonify({"ok": True, "id": data["id"]})


@app.route("/saved/request/<rid>", methods=["PUT"])
def update_request(rid):
    data = request.get_json() or {}
    db = load_db()
    for r in db["requests"]:
        if r["id"] == rid:
            if "name" in data:
                r["name"] = data["name"].strip() or r["name"]
            if "folder_id" in data:
                r["folder_id"] = data["folder_id"]
            r["updated_at"] = now_iso()
            break
    save_db(db)
    return jsonify({"ok": True})


@app.route("/saved/request/<rid>", methods=["DELETE"])
def delete_request(rid):
    db = load_db()
    db["requests"] = [r for r in db["requests"] if r["id"] != rid]
    save_db(db)
    return jsonify({"ok": True})


# ── RUNNER ──────────────────────────────────────────────────

@app.route("/run", methods=["POST"])
def run_job():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Ma'lumot yuborilmagan"}), 400
    if not data.get("url"):
        return jsonify({"error": "URL kiritilmagan"}), 400
    if not data.get("rows"):
        data["rows"] = [{}]   # faylsiz — bitta so'rov

    job_id     = str(uuid.uuid4())
    stop_event = threading.Event()
    result_q   = queue_module.Queue()

    with active_jobs_lock:
        active_jobs[job_id] = {
            "config":     data,
            "queue":      result_q,
            "stop_event": stop_event,
        }

    threading.Thread(target=job_worker, args=(job_id,), daemon=True).start()
    return jsonify({"job_id": job_id})


@app.route("/stream/<job_id>")
def stream(job_id):
    with active_jobs_lock:
        job = active_jobs.get(job_id)

    if not job:
        def err():
            yield 'data: {"error":"Job topilmadi"}\n\n'
        return Response(err(), content_type="text/event-stream")

    q = job["queue"]

    def generate():
        while True:
            try:
                item = q.get(timeout=60)
            except queue_module.Empty:
                yield ": keepalive\n\n"
                continue
            etype   = item["type"]
            payload = json.dumps(item["data"], ensure_ascii=False)
            yield f"event: {etype}\ndata: {payload}\n\n"
            if etype == "done":
                with active_jobs_lock:
                    active_jobs.pop(job_id, None)
                break

    return Response(
        stream_with_context(generate()),
        content_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


@app.route("/stop/<job_id>", methods=["POST"])
def stop_job(job_id):
    with active_jobs_lock:
        job = active_jobs.get(job_id)
    if job:
        job["stop_event"].set()
        return jsonify({"ok": True})
    return jsonify({"error": "Job topilmadi"}), 404


# ============================================================
# START
# ============================================================

def open_browser():
    time.sleep(0.9)
    webbrowser.open("http://127.0.0.1:5050")


if __name__ == "__main__":
    threading.Thread(target=open_browser, daemon=True).start()
    print()
    print("=" * 50)
    print("  API RUNNER  —  http://127.0.0.1:5050")
    print("=" * 50)
    print()
    app.run(host="127.0.0.1", port=5050, debug=False, threaded=True)
