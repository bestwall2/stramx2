import subprocess, threading, os, time, logging, json, base64, sys, signal, shlex, re
from collections import deque
from flask import Flask, request, jsonify, Response

# HF Space containers buffer stdout/stderr by default, which can make log
# lines appear delayed or missing entirely in the Logs viewer until the
# buffer flushes or the process exits. Force unbuffered, immediate output.
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    stream=sys.stdout,
    force=True,
)
log = logging.getLogger(__name__)

app = Flask(__name__)

MAX_STREAMS = 10
RTMP_BASE = "rtmps://live-api-s.facebook.com:443/rtmp/"

# Facebook's RTMP ingest needs this long to fully release a stream key
# after a disconnect before it will accept a new connection on the same key.
# Applies to: initial start, manual restart, and crash auto-retry.
RECONNECT_DELAY = 35  # seconds

# Facebook stream keys allow a maximum of 4 hours of cumulative active
# streaming. After that the ingest rejects new data on the same key.
# We track actual FFmpeg uptime (not wall-clock) so pauses don't count.
MAX_STREAM_SECONDS = 4 * 3600   # 14400s — cumulative active streaming limit
KEY_LIFETIME_SECONDS = 8 * 3600  # 28800s — wall-clock lifetime from creation

# ── Pipeline tuning (ffmpeg | mbuffer | ffmpeg) ──
# mbuffer absorbs brief source drop-outs so they don't propagate straight
# through to Facebook. Bigger = more resilience to long outages, but more
# added latency on the live broadcast.
MBUFFER_SIZE = os.environ.get("RELAY_MBUFFER_SIZE", "96M")

# ── Proactive drift-protection watchdog ──
# Some sources spam corrupt packets / timestamp discontinuities so often
# that ffmpeg's internal offset correction keeps stacking rather than
# settling, causing A/V drift to slowly get worse the longer a session
# runs. If we see too many "bad" lines in a short window, we proactively
# kill and restart the pipeline (same 35s wait as a normal restart) rather
# than let the drift accumulate for the rest of the session.
ERROR_WINDOW_SECONDS = int(os.environ.get("RELAY_ERROR_WINDOW", "60"))
ERROR_THRESHOLD = int(os.environ.get("RELAY_ERROR_THRESHOLD", "30"))
# Ignore errors for this long after launch: startup is noisy and must not
# trigger a restart loop before the stream ever gets going.
STARTUP_GRACE_SECONDS = int(os.environ.get("RELAY_STARTUP_GRACE", "60"))

# Count one match per real source hiccup, not every log line it produces
# (a single hiccup prints 4-5 lines: premature end, corrupt packet, DTS
# out of order, discontinuity...).
BAD_LINE_RE = re.compile(
    r"(timestamp discontinuity|Stream ends prematurely)",
    re.IGNORECASE,
)

# Simple shared-secret so randoms who find your HF url can't control streams.
# Set this as an env var on your HF Space (Settings -> Repository secrets).
CONTROL_SECRET = os.environ.get("CONTROL_SECRET", "changeme")

# Stream registry
# { id: { name, input_url, stream_key, process, thread, running, restarts, status } }
streams = {}
streams_lock = threading.Lock()

# ─────────────────────────────────────────
# Pipeline command + process-group helpers
# ─────────────────────────────────────────

def clean_key(raw: str) -> str:
    """Facebook stream keys only contain letters, digits, '-' and '_'.
    Drops anything else (e.g. a trailing '|' picked up when copying)."""
    return re.sub(r'[^A-Za-z0-9_\-]', '', raw or '')


def build_pipeline_cmd(input_url: str, stream_key: str) -> str:
    """
    Builds the ffmpeg | mbuffer | ffmpeg pipeline as a single shell string,
    run via `bash -c`. Values are shell-quoted with shlex.quote to avoid
    shell-injection from input_url / stream_key (these arrive as HTTP query
    params, so treat them as untrusted even though the endpoint is
    secret-gated).
    """
    output_url = f"{RTMP_BASE}{stream_key}"
    safe_input = shlex.quote(input_url)
    safe_output = shlex.quote(output_url)
    safe_mbuffer = shlex.quote(MBUFFER_SIZE)

    return f'''
set -o pipefail
ffmpeg \\
  -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 \\
  -reconnect_at_eof 1 -reconnect_on_network_error 1 \\
  -timeout 10000000 -thread_queue_size 1024 \\
  -i {safe_input} \\
  -fflags +genpts+discardcorrupt+igndts -err_detect ignore_err \\
  -c copy -f mpegts - \\
| mbuffer -q -m {safe_mbuffer} \\
| ffmpeg -re -f mpegts -i - \\
  -max_muxing_queue_size 1024 -max_interleave_delta 0 \\
  -c:v copy -fps_mode passthrough \\
  -c:a aac -b:a 128k -ar 48000 -ac 2 -af aresample=async=1:min_hard_comp=0.100000:first_pts=0 \\
  -avoid_negative_ts make_zero \\
  -f flv {safe_output} 2>&1
'''.strip()


def kill_process_group(proc, timeout=5):
    """
    Kills the ENTIRE pipeline (both ffmpeg stages + mbuffer), not just the
    bash wrapper process. The pipeline is launched with preexec_fn=os.setsid
    so all of bash's pipeline children share its process group — killing
    only proc itself (e.g. proc.terminate()) can leave earlier pipeline
    stages running as orphans.
    """
    if proc is None:
        return
    try:
        pgid = os.getpgid(proc.pid)
    except ProcessLookupError:
        return
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=5)
        except Exception:
            pass

# ─────────────────────────────────────────
# Core pipeline runner
# ─────────────────────────────────────────

def run_stream(stream_id):
    while True:
        with streams_lock:
            s = streams.get(stream_id)
            if not s or not s["running"]:
                break
            input_url = s["input_url"]
            stream_key = s["stream_key"]

        try:
            cmd_str = build_pipeline_cmd(input_url, stream_key)

            # Wait before every connection attempt — Facebook needs time to
            # release the stream key after any disconnect (including the
            # very first start, restart, or crash recovery).
            log.info(f"[{stream_id}] Waiting {RECONNECT_DELAY}s for Facebook stream key to reset...")
            with streams_lock:
                streams[stream_id]["status"] = "waiting"

            for remaining in range(RECONNECT_DELAY, 0, -1):
                with streams_lock:
                    if not streams.get(stream_id, {}).get("running"):
                        log.info(f"[{stream_id}] Stopped during wait — aborting")
                        streams[stream_id]["status"] = "stopped"
                        return
                time.sleep(1)

            with streams_lock:
                if not streams.get(stream_id, {}).get("running"):
                    streams[stream_id]["status"] = "stopped"
                    break

            log.info(f"[{stream_id}] Starting pipeline: {input_url[:60]}")

            with streams_lock:
                streams[stream_id]["status"] = "running"

            try:
                proc = subprocess.Popen(
                    ["bash", "-c", cmd_str],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    preexec_fn=os.setsid,  # new process group -> can kill whole pipeline
                )
            except Exception as e:
                log.error(f"[{stream_id}] FAILED TO LAUNCH pipeline: {e}")
                with streams_lock:
                    streams[stream_id]["status"] = "stopped"
                    streams[stream_id]["running"] = False
                break

            with streams_lock:
                streams[stream_id]["process"] = proc

            # ── Streaming time tracker + drift watchdog ──
            # Poll every second instead of blocking on communicate() so we
            # can measure actual uptime, enforce the 4h/8h limits, and react
            # to a high error rate from the reader thread below.
            session_start = time.time()
            output_chunks = deque(maxlen=60)  # last lines only, bounded memory
            error_times = deque()
            force_restart = threading.Event()

            def _read_output():
                for raw_line in iter(proc.stdout.readline, b''):
                    if not raw_line:
                        break
                    output_chunks.append(raw_line)
                    line = raw_line.decode(errors='replace').rstrip('\n')
                    if not line:
                        continue

                    now = time.time()
                    if now - session_start < STARTUP_GRACE_SECONDS:
                        continue  # startup noise doesn't count
                    if BAD_LINE_RE.search(line):
                        error_times.append(now)
                        cutoff = now - ERROR_WINDOW_SECONDS
                        while error_times and error_times[0] < cutoff:
                            error_times.popleft()

                        if len(error_times) >= ERROR_THRESHOLD:
                            log.warning(
                                f"[{stream_id}] Error rate too high "
                                f"({len(error_times)} bad lines / {ERROR_WINDOW_SECONDS}s) "
                                f"— forcing restart to reset timestamp drift"
                            )
                            force_restart.set()
                            return

            output_thread = threading.Thread(target=_read_output, daemon=True)
            output_thread.start()

            expired = False
            expired_reason = ""
            proactive_restart = False

            while proc.poll() is None:
                time.sleep(1)
                session_elapsed = time.time() - session_start
                with streams_lock:
                    total_streamed = streams[stream_id]["streamed_seconds"] + session_elapsed
                    wall_age = time.time() - streams[stream_id]["created_at"]
                    still_running = streams[stream_id]["running"]

                if not still_running:
                    break

                if total_streamed >= MAX_STREAM_SECONDS:
                    log.warning(f"[{stream_id}] 4-hour streaming limit reached — stopping permanently")
                    kill_process_group(proc)
                    expired = True
                    expired_reason = "stream_limit"
                    break

                if wall_age >= KEY_LIFETIME_SECONDS:
                    log.warning(f"[{stream_id}] 8-hour key lifetime expired — stopping permanently")
                    kill_process_group(proc)
                    expired = True
                    expired_reason = "key_expired"
                    break

                if force_restart.is_set():
                    log.warning(f"[{stream_id}] Proactively restarting pipeline (error-rate watchdog)")
                    kill_process_group(proc)
                    proactive_restart = True
                    break

            proc.wait()
            output_thread.join(timeout=5)
            output_tail = b''.join(output_chunks)
            exit_code = proc.returncode if proc.returncode is not None else -1

            # Save how long this session actually streamed
            session_elapsed = time.time() - session_start
            with streams_lock:
                streams[stream_id]["streamed_seconds"] = min(
                    streams[stream_id]["streamed_seconds"] + session_elapsed,
                    MAX_STREAM_SECONDS
                )

            # Expired: do not restart, mark permanently done
            if expired:
                with streams_lock:
                    streams[stream_id]["running"] = False
                    streams[stream_id]["status"] = "expired"
                log.info(f"[{stream_id}] Stream expired ({expired_reason}). Must add a new stream key.")
                break

            with streams_lock:
                if not streams.get(stream_id, {}).get("running"):
                    log.info(f"[{stream_id}] Stopped by user")
                    streams[stream_id]["status"] = "stopped"
                    break
                streams[stream_id]["restarts"] += 1
                restart_count = streams[stream_id]["restarts"]

            if exit_code == 0 and not proactive_restart:
                log.info(f"[{stream_id}] Pipeline exited cleanly")
                with streams_lock:
                    streams[stream_id]["status"] = "stopped"
                break

            if proactive_restart:
                log.warning(f"[{stream_id}] Restarting (proactive drift reset #{restart_count}) "
                            f"after {RECONNECT_DELAY}s")
            else:
                log.warning(f"[{stream_id}] Pipeline crashed (code {exit_code}), "
                            f"retry #{restart_count} after {RECONNECT_DELAY}s")
            log.warning(f"[{stream_id}] output tail:\n{output_tail[-1500:].decode(errors='replace')}")

            with streams_lock:
                streams[stream_id]["status"] = "restarting"
            # No extra sleep here — the RECONNECT_DELAY at the top of
            # the next loop iteration covers the wait before reconnecting.

        except Exception as e:
            log.exception(f"[{stream_id}] Unexpected error in run_stream: {e}")
            with streams_lock:
                if stream_id in streams:
                    streams[stream_id]["status"] = "stopped"
                    streams[stream_id]["running"] = False
            break

# ─────────────────────────────────────────
# OG response builder
# ─────────────────────────────────────────

def og_response(payload: dict, status: int = 200):
    compact = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    if len(compact) > 280:
        log.warning(f"Payload too large for OG budget ({len(compact)} chars)")
        compact = json.dumps({"ok": 0, "e": "payload_too_large"}, separators=(",", ":"))
        status = 500
    b64 = base64.b64encode(compact.encode('utf-8')).decode('ascii')
    html = f"""<!DOCTYPE html>
<html><head>
<title>{b64}</title>
<meta property="og:title" content="{b64}" />
<meta property="og:site_name" content="{b64}" />
<meta property="og:type" content="website" />
<meta property="og:url" content="{request.url}" />
</head><body><pre>{compact}</pre></body></html>"""
    return Response(html, mimetype='text/html', status=status)

def is_scraper():
    ua = request.headers.get('User-Agent', '').lower()
    return 'facebook' in ua or 'fb' in ua or 'graph' in ua or request.args.get('format') == 'og'

def reply(payload: dict, status: int = 200):
    if is_scraper():
        return og_response(payload, status)
    return jsonify(payload), status

def _reaper():
    """
    Runs every 60s. Marks stopped/idle streams as 'expired' when their
    8-hour wall-clock lifetime is up, even if they were never started or
    stopped early. Without this, an idle stream would only get an expired
    status when someone tries to start it.
    """
    while True:
        time.sleep(60)
        now = time.time()
        with streams_lock:
            for sid, v in streams.items():
                if v["status"] != "expired" and not v["running"]:
                    if now - v.get("created_at", now) >= KEY_LIFETIME_SECONDS:
                        v["status"] = "expired"
                        log.info(f"[{sid}] Stream key wall-clock expired (8h). Marked by reaper.")

threading.Thread(target=_reaper, daemon=True, name="reaper").start()

# ─────────────────────────────────────────
# API
# ─────────────────────────────────────────

@app.route('/api', methods=['GET'])
def api():
    if request.args.get('secret') != CONTROL_SECRET:
        return reply({"ok": 0, "e": " هاتفك لايدعم تطبيقنا ❌ "}, 401)

    action = request.args.get('action', '')

    if action == 'list':
        with streams_lock:
            all_items = [{"i": sid, "n": v["name"][:12], "st": v["status"]} for sid, v in streams.items()]
        included = []
        more = False
        for item in all_items:
            trial = {"ok": 1, "act": "list", "s": included + [item], "more": 0}
            if len(json.dumps(trial, separators=(",", ":"))) > 260:
                more = True
                break
            included.append(item)
        result = {"ok": 1, "act": "list", "s": included}
        if more:
            result["more"] = 1
        return reply(result)

    # Full listing (no OG size budget concerns — used by the dashboard UI,
    # never by the Facebook scraper) so the UI can render everything in
    # one request instead of one 'get' call per stream.
    if action == 'list_full':
        with streams_lock:
            data = []
            for sid, v in streams.items():
                data.append({
                    "i": sid,
                    "n": v["name"],
                    "st": v["status"],
                    "run": int(v["running"]),
                    "rc": v["restarts"],
                    "url": v["input_url"],
                    "key_tail": v["stream_key"][-6:] if v["stream_key"] else "",
                    "sec": int(v.get("streamed_seconds", 0)),
                    "rem": max(0, int(MAX_STREAM_SECONDS - v.get("streamed_seconds", 0))),
                    "wall_rem": max(0, int(KEY_LIFETIME_SECONDS - (time.time() - v.get("created_at", time.time())))),
                })
        return reply({"ok": 1, "act": "list_full", "s": data})

    if action == 'get':
        sid = request.args.get('id', '')
        with streams_lock:
            if sid not in streams:
                return reply({"ok": 0, "e": "notfound"}, 404)
            v = streams[sid]
            data = {
                "ok": 1, "act": "get", "i": sid, "n": v["name"],
                "st": v["status"], "rc": v["restarts"], "run": int(v["running"]),
                "url": v["input_url"][:60],
                "sec": int(v.get("streamed_seconds", 0)),
                "rem": max(0, int(MAX_STREAM_SECONDS - v.get("streamed_seconds", 0))),
                "wall_rem": max(0, int(KEY_LIFETIME_SECONDS - (time.time() - v.get("created_at", time.time())))),
            }
        return reply(data)

    if action == 'add':
        name = request.args.get('name', '').strip()
        input_url = request.args.get('input_url', '').strip()
        stream_key = clean_key(request.args.get('stream_key', ''))
        if not name or not input_url or not stream_key:
            return reply({"ok": 0, "e": "missing_fields"}, 400)
        with streams_lock:
            for existing_id, v in streams.items():
                if (v["name"] == name and v["input_url"] == input_url
                        and v["stream_key"] == stream_key):
                    log.info(f"Duplicate add suppressed, returning existing id={existing_id}")
                    return reply({"ok": 1, "act": "add", "i": existing_id, "dup": 1}, 200)
            if len(streams) >= MAX_STREAMS:
                return reply({"ok": 0, "e": "max_streams"}, 400)
            existing_ids = set(streams.keys())
            sid = next(str(i) for i in range(1, MAX_STREAMS + 1) if str(i) not in existing_ids)
            streams[sid] = {
                "name": name, "input_url": input_url, "stream_key": stream_key,
                "process": None, "thread": None, "running": False,
                "restarts": 0, "status": "stopped",
                "streamed_seconds": 0,   # cumulative active streaming time across sessions
                "created_at": time.time(),  # wall-clock expiry anchor
            }
        log.info(f"Stream added: id={sid} name={name}")
        return reply({"ok": 1, "act": "add", "i": sid}, 201)

    # Edit an existing stream's name / input URL / stream key. Only allowed
    # while stopped, so we never rewrite the config FFmpeg is mid-flight on.
    # Changing the stream key resets the usage counters, since a new key
    # means a fresh 4h/8h budget from Facebook.
    if action == 'update':
        sid = request.args.get('id', '')
        with streams_lock:
            if sid not in streams:
                return reply({"ok": 0, "e": "notfound"}, 404)
            v = streams[sid]
            if v["running"]:
                return reply({"ok": 0, "e": "stop_first"}, 400)

            name = request.args.get('name', '').strip()
            input_url = request.args.get('input_url', '').strip()
            stream_key = clean_key(request.args.get('stream_key', ''))

            if not name and not input_url and not stream_key:
                return reply({"ok": 0, "e": "missing_fields"}, 400)

            if name:
                v["name"] = name
            if input_url:
                v["input_url"] = input_url
            if stream_key:
                v["stream_key"] = stream_key
                v["streamed_seconds"] = 0
                v["created_at"] = time.time()
                v["status"] = "stopped"
        log.info(f"Stream updated: id={sid}")
        return reply({"ok": 1, "act": "update", "i": sid})

    if action == 'delete':
        sid = request.args.get('id', '')
        with streams_lock:
            if sid not in streams:
                return reply({"ok": 0, "e": "notfound"}, 404)
            v = streams[sid]
            v["running"] = False
            proc = v["process"]
            del streams[sid]
        if proc and proc.poll() is None:
            kill_process_group(proc)
        log.info(f"Stream removed: id={sid}")
        return reply({"ok": 1, "act": "delete", "i": sid})

    if action == 'start':
        sid = request.args.get('id', '')
        with streams_lock:
            if sid not in streams:
                return reply({"ok": 0, "e": "notfound"}, 404)
            v = streams[sid]
            if v["status"] == "expired":
                return reply({"ok": 0, "e": "key_expired"}, 400)
            if time.time() - v["created_at"] >= KEY_LIFETIME_SECONDS:
                v["status"] = "expired"
                return reply({"ok": 0, "e": "key_expired"}, 400)
            if v["running"]:
                return reply({"ok": 1, "act": "start", "i": sid, "st": "already"})
            v["running"] = True
            v["restarts"] = 0
            v["status"] = "waiting"
            t = threading.Thread(target=run_stream, args=(sid,), daemon=True)
            v["thread"] = t
            t.start()
        log.info(f"Stream start requested: id={sid} (35s wait begins in background)")
        return reply({"ok": 1, "act": "start", "i": sid})

    # 'stop' doubles as pause: streamed_seconds is preserved, so hitting
    # start again later resumes against the same 4h/8h budget.
    if action == 'stop':
        sid = request.args.get('id', '')
        with streams_lock:
            if sid not in streams:
                return reply({"ok": 0, "e": "notfound"}, 404)
            v = streams[sid]
            v["running"] = False
            v["status"] = "stopping"
            proc = v["process"]
        if proc and proc.poll() is None:
            kill_process_group(proc)
        log.info(f"Stream stopped: id={sid}")
        return reply({"ok": 1, "act": "stop", "i": sid})

    if action == 'restart':
        sid = request.args.get('id', '')
        with streams_lock:
            if sid not in streams:
                return reply({"ok": 0, "e": "notfound"}, 404)
            v = streams[sid]
            v["running"] = False
            v["status"] = "stopping"
            proc = v["process"]
        if proc and proc.poll() is None:
            kill_process_group(proc)
        time.sleep(2)  # brief pause to let the old pipeline fully die
        with streams_lock:
            v = streams[sid]
            v["running"] = True
            v["restarts"] = 0
            v["status"] = "waiting"
            t = threading.Thread(target=run_stream, args=(sid,), daemon=True)
            v["thread"] = t
            t.start()
        log.info(f"Stream restart requested: id={sid} (35s wait begins in background)")
        return reply({"ok": 1, "act": "restart", "i": sid})

    return reply({"ok": 0, "e": "bad_action"}, 400)

@app.route('/ping')
def ping():
    return 'ok'

# ─────────────────────────────────────────
# Dashboard UI — served from this same app, same origin, so the control
# secret never has to leave your own server's domain.
# ─────────────────────────────────────────

@app.route('/')
def dashboard():
    return Response(DASHBOARD_HTML, mimetype='text/html')

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Relay Console</title>
<style>
  :root{
    --bg:#14171a; --panel:#1c2024; --panel2:#22262b; --line:#2c3136;
    --text:#e8e6e1; --sub:#8b9198; --live:#4caf7d; --wait:#d9a441;
    --idle:#5b6167; --err:#d9534f; --accent:#5b8bd9;
  }
  *{box-sizing:border-box;}
  body{margin:0;background:var(--bg);color:var(--text);
    font-family:-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
    padding:28px 20px 80px;}
  .wrap{max-width:860px;margin:0 auto;}
  h1{font-size:20px;font-weight:600;margin:0 0 2px;letter-spacing:.2px;}
  .tag{color:var(--sub);font-size:13px;margin:0 0 20px;}
  .authbar{display:flex;gap:8px;margin-bottom:24px;}
  .authbar input{flex:1;}
  input,button{font:inherit;}
  input[type=text],input[type=password],input[type=url]{
    background:var(--panel2);border:1px solid var(--line);color:var(--text);
    border-radius:6px;padding:9px 11px;font-size:14px;width:100%;}
  input:focus{outline:none;border-color:var(--accent);}
  button{cursor:pointer;border-radius:6px;border:1px solid var(--line);
    background:var(--panel2);color:var(--text);padding:8px 13px;font-size:13px;
    transition:border-color .15s, background .15s;}
  button:hover{border-color:var(--accent);}
  button:disabled{opacity:.4;cursor:default;}
  button.primary{background:var(--accent);border-color:var(--accent);color:#0d1117;font-weight:600;}
  button.danger:hover{border-color:var(--err);color:var(--err);}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:10px;
    padding:16px 18px;margin-bottom:14px;}
  .addform{display:grid;gap:10px;grid-template-columns:1fr 1fr;margin-bottom:24px;}
  .addform .full{grid-column:1/-1;}
  .addform button{grid-column:1/-1;}
  .row{display:flex;align-items:center;gap:10px;}
  .dot{width:9px;height:9px;border-radius:50%;flex-shrink:0;}
  .dot.running{background:var(--live);box-shadow:0 0 6px var(--live);}
  .dot.waiting,.dot.restarting,.dot.stopping{background:var(--wait);}
  .dot.stopped{background:var(--idle);}
  .dot.expired{background:var(--err);}
  .name{font-weight:600;font-size:15px;}
  .status{font-size:12px;color:var(--sub);text-transform:capitalize;}
  .meta{font-size:12px;color:var(--sub);margin-top:6px;line-height:1.6;
    display:flex;flex-wrap:wrap;gap:14px;}
  .actions{display:flex;gap:6px;margin-left:auto;flex-wrap:wrap;}
  .editrow{display:none;margin-top:12px;padding-top:12px;border-top:1px solid var(--line);
    display:grid;gap:8px;grid-template-columns:1fr 1fr;}
  .editrow.open{display:grid;}
  .editrow input{font-size:13px;}
  .editrow .full{grid-column:1/-1;}
  .editactions{grid-column:1/-1;display:flex;gap:8px;}
  .empty{color:var(--sub);font-size:13px;padding:20px 0;text-align:center;}
  .err{color:var(--err);font-size:13px;margin-top:8px;min-height:16px;}
  label{font-size:11px;color:var(--sub);display:block;margin-bottom:4px;}
</style>
</head>
<body>
<div class="wrap">
  <h1>Relay Console</h1>
  <p class="tag">Facebook RTMP relay — add, start, pause, restart, edit and remove streams.</p>

  <div class="authbar">
    <input id="secret" type="password" placeholder="Control secret">
    <button class="primary" onclick="connect()">Connect</button>
  </div>
  <div id="authErr" class="err"></div>

  <div id="app" style="display:none;">
    <div class="card">
      <label style="margin-bottom:8px;">Add stream</label>
      <div class="addform">
        <div><label>Name</label><input id="a_name" type="text" placeholder="e.g. Camera 1"></div>
        <div><label>Stream key</label><input id="a_key" type="text" placeholder="Facebook stream key"></div>
        <div class="full"><label>Input URL</label><input id="a_url" type="text" placeholder="http(s)://... source stream"></div>
        <button onclick="addStream()">Add stream</button>
      </div>
      <div id="addErr" class="err"></div>
    </div>

    <div id="list"></div>
    <div id="emptyMsg" class="empty" style="display:none;">No streams yet — add one above.</div>
  </div>
</div>

<script>
let SECRET = localStorage.getItem('relay_secret') || '';
let poll = null;

function api(action, params={}){
  const q = new URLSearchParams({action, secret: SECRET, ...params});
  return fetch('/api?' + q.toString()).then(r => r.json());
}

function connect(){
  const s = document.getElementById('secret').value.trim() || SECRET;
  SECRET = s;
  api('list_full').then(res => {
    if(!res.ok){
      document.getElementById('authErr').textContent = 'Wrong secret, or server unreachable.';
      document.getElementById('app').style.display = 'none';
      return;
    }
    localStorage.setItem('relay_secret', SECRET);
    document.getElementById('authErr').textContent = '';
    document.getElementById('app').style.display = 'block';
    render(res.s);
    if(!poll) poll = setInterval(refresh, 4000);
  }).catch(() => {
    document.getElementById('authErr').textContent = 'Could not reach the server.';
  });
}

function refresh(){
  api('list_full').then(res => { if(res.ok) render(res.s); });
}

function fmtTime(sec){
  const h = Math.floor(sec/3600), m = Math.floor((sec%3600)/60);
  return h + 'h ' + m + 'm';
}

function render(streams){
  const list = document.getElementById('list');
  document.getElementById('emptyMsg').style.display = streams.length ? 'none' : 'block';
  list.innerHTML = streams.map(s => {
    const canStart = !s.run && s.st !== 'expired';
    return `
    <div class="card" id="card_${s.i}">
      <div class="row">
        <div class="dot ${s.st}"></div>
        <div>
          <div class="name">${escapeHtml(s.n)}</div>
          <div class="status">${s.st}${s.rc ? ' · ' + s.rc + ' retries' : ''}</div>
        </div>
        <div class="actions">
          <button ${canStart ? '' : 'disabled'} onclick="act('start','${s.i}')">Start</button>
          <button ${s.run ? '' : 'disabled'} onclick="act('stop','${s.i}')">Pause</button>
          <button onclick="act('restart','${s.i}')">Restart</button>
          <button onclick="toggleEdit('${s.i}')">Edit</button>
          <button class="danger" onclick="removeStream('${s.i}')">Remove</button>
        </div>
      </div>
      <div class="meta">
        <span>Streamed ${fmtTime(s.sec)} / 4h</span>
        <span>Key expires in ${fmtTime(s.wall_rem)}</span>
        <span>Key •••${s.key_tail}</span>
      </div>
      <div class="editrow" id="edit_${s.i}">
        <div><label>New name</label><input id="e_name_${s.i}" type="text" placeholder="${escapeHtml(s.n)}"></div>
        <div><label>New stream key</label><input id="e_key_${s.i}" type="text" placeholder="unchanged"></div>
        <div class="full"><label>New input URL</label><input id="e_url_${s.i}" type="text" placeholder="${escapeHtml(s.url)}"></div>
        <div class="editactions">
          <button class="primary" onclick="saveEdit('${s.i}')">Save changes</button>
          <button onclick="toggleEdit('${s.i}')">Cancel</button>
        </div>
        <div class="full err" id="editErr_${s.i}"></div>
      </div>
    </div>`;
  }).join('');
}

function escapeHtml(str){
  return String(str).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

function act(action, id){
  api(action, {id}).then(refresh);
}

function removeStream(id){
  if(!confirm('Remove this stream? This cannot be undone.')) return;
  api('delete', {id}).then(refresh);
}

function toggleEdit(id){
  document.getElementById('edit_' + id).classList.toggle('open');
}

function saveEdit(id){
  const name = document.getElementById('e_name_' + id).value.trim();
  const key = document.getElementById('e_key_' + id).value.trim();
  const url = document.getElementById('e_url_' + id).value.trim();
  const errEl = document.getElementById('editErr_' + id);
  errEl.textContent = '';
  api('update', {id, name, stream_key: key, input_url: url}).then(res => {
    if(!res.ok){
      errEl.textContent = res.e === 'stop_first'
        ? 'Pause the stream before editing it.'
        : 'Could not save (' + res.e + ').';
      return;
    }
    toggleEdit(id);
    refresh();
  });
}

function addStream(){
  const name = document.getElementById('a_name').value.trim();
  const input_url = document.getElementById('a_url').value.trim();
  const stream_key = document.getElementById('a_key').value.trim();
  const errEl = document.getElementById('addErr');
  errEl.textContent = '';
  if(!name || !input_url || !stream_key){
    errEl.textContent = 'Fill in name, input URL and stream key.';
    return;
  }
  api('add', {name, input_url, stream_key}).then(res => {
    if(!res.ok){
      errEl.textContent = res.e === 'max_streams' ? 'Limit of 10 streams reached.' : 'Could not add (' + res.e + ').';
      return;
    }
    document.getElementById('a_name').value = '';
    document.getElementById('a_url').value = '';
    document.getElementById('a_key').value = '';
    refresh();
  });
}

if(SECRET){
  document.getElementById('secret').value = SECRET;
  connect();
}
</script>
</body>
</html>
"""

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 7860)), debug=False)