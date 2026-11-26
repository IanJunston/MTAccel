"""冒烟检查：模拟 启动面板.bat 的调用方式，确认 `python app\\server.py` 能独立起服务。"""
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PORT = 8770

proc = subprocess.Popen(
    [sys.executable, "app/server.py", "--no-browser", "--port", str(PORT)],
    cwd=str(ROOT),
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    encoding="utf-8",
    errors="replace",
)
ok = False
last = None
try:
    for _ in range(40):
        time.sleep(0.4)
        if proc.poll() is not None:
            last = f"进程已退出 code={proc.returncode}"
            break
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{PORT}/", timeout=2) as r:
                body = r.read().decode("utf-8", "replace")
                has = ("chunkMap" in body) and ("speedChart" in body) and ("EventSource" in body)
                print(f"[首页] status={r.status} bytes={len(body)} 关键元素={has}")
                with urllib.request.urlopen(f"http://127.0.0.1:{PORT}/api/state", timeout=2) as r2:
                    print(f"[状态] {r2.read().decode('utf-8')[:160]}")
                ok = True
                break
        except Exception as e:  # noqa: BLE001
            last = e
finally:
    proc.terminate()
    try:
        out = proc.communicate(timeout=8)[0]
    except Exception:  # noqa: BLE001
        proc.kill()
        out = ""
    if out:
        print("[输出]", out.strip()[:400])

print("结果:", "通过" if ok else f"失败 {last}")
sys.exit(0 if ok else 1)
