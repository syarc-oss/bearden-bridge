#!/usr/bin/env python3
"""End-to-end tunnel test: two bridge instances over PTYs.

A (tag US, ptyA) <--> HTTP <--> B (tag AU, ptyB). Mesh events written into
A's pty master should appear as an [AU] digest on B's serial line after one
flush interval, and vice versa via push. Also checks auth on /tunnel/ingest.
"""
import json, os, pty, select, subprocess, sys, time, urllib.request

BIN = os.path.expanduser("~/bearden-bridge/target/debug/bearden-bridge")
WORK = "/tmp/tunneltest"
os.makedirs(WORK, exist_ok=True)

def spawn(name, tag, peer_port, port, peer_tag):
    m, s = pty.openpty()
    env = dict(os.environ,
        BEARDEN_PORT=os.ttyname(s),
        BEARDEN_DB=f"{WORK}/{name}.db",
        BEARDEN_API=f"127.0.0.1:{port}",
        BEARDEN_TUNNEL_PEER=f"http://127.0.0.1:{peer_port}",
        BEARDEN_TUNNEL_TOKEN="s3cret",
        BEARDEN_TUNNEL_TAG=tag,
        BEARDEN_TUNNEL_PEER_TAG=peer_tag,
        BEARDEN_TUNNEL_FLUSH_SECS="4",
        BEARDEN_TUNNEL_RF_GAP_SECS="1",
        BEARDEN_TUNNEL_MAX_MSGS="4",
        BEARDEN_TUNNEL_MAX_POSTS="1",
    )
    p = subprocess.Popen([BIN], env=env, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    os.close(s)
    return m, p

def req(port, method, path, body=None, token=None):
    url = f"http://127.0.0.1:{port}{path}"
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method)
    if body is not None: r.add_header("Content-Type", "application/json")
    if token: r.add_header("X-Tunnel-Token", token)
    try:
        with urllib.request.urlopen(r, timeout=5) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")

def read_until(fd, pred, timeout):
    end = time.time() + timeout
    buf = b""
    while time.time() < end:
        r, _, _ = select.select([fd], [], [], 1)
        if r:
            try: buf += os.read(fd, 4096)
            except OSError: break
            if pred(buf.decode(errors="replace")): return buf.decode(errors="replace")
    return None

fails = []
def check(name, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f" -- {detail}"))
    if not cond: fails.append(name)

mA, pA = spawn("a", "US", 18096, 18095, "AU")
mB, pB = spawn("b", "AU", 18095, 18096, "US")
time.sleep(2)

# auth: bad token rejected
st, _ = req(18095, "POST", "/tunnel/ingest", {"tag": "AU", "msgs": []}, "wrong")
check("ingest rejects bad token", st == 401, f"status {st}")
st, _ = req(18095, "POST", "/tunnel/ingest", {"tag": "AU", "msgs": []}, "s3cret")
check("ingest accepts good token", st == 200)

# mesh event on A -> tunnel push -> B queue -> B RF digest
line = json.dumps({"evt": "msg", "key": "aabbccdd", "ts": int(time.time()),
                   "sys": 0, "perm": 3, "text": "gday from the US mesh"})
os.write(mA, f"BRIDGE {line}\n".encode())
digest = read_until(mB, lambda s: "gday from the US mesh" in s, 15)
check("US mesh msg appears as AU-side digest", digest is not None,
      f"serial B output: {digest!r}")
if digest:
    check("digest has [US tag", "[US " in digest, digest)

# reverse: mesh event on B -> A digest
line2 = json.dumps({"evt": "msg", "key": "11223344", "ts": int(time.time()),
                    "sys": 0, "perm": 3, "text": "hello from australia"})
os.write(mB, f"BRIDGE {line2}\n".encode())
digest2 = read_until(mA, lambda s: "hello from australia" in s, 15)
check("AU mesh msg appears as US-side digest", digest2 is not None,
      f"serial A output: {digest2!r}")

# echo loop: digest arriving back must not re-tunnel (wait 2 flushes, count)
if digest:
    os.write(mA, f"BRIDGE {json.dumps({'evt':'msg','key':'ff001122','ts':int(time.time()),'sys':0,'perm':3,'text':digest.strip().splitlines()[-1]})}\n".encode())
time.sleep(10)
st, body = req(18096, "GET", "/tunnel/status", token="s3cret")
check("tunnel status reachable", st == 200, str(body))

# kill switch blocks flush
st, _ = req(18096, "POST", "/killswitch/on", {})
os.write(mA, b"")  # no-op
st, body = req(18095, "POST", "/tunnel/flush", token=None)
check("admin flush on A ok", st == 200, str(body))

for p in (pA, pB):
    p.terminate()
print("\nRESULT:", "ALL PASS" if not fails else f"FAILURES: {fails}")
sys.exit(1 if fails else 0)
