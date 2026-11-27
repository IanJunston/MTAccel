# MTAccel · Multi-threaded Download Accelerator

**A downloader that finds the best concurrency by itself.** Give it a direct link; it probes the server and your network, decides how many connections to open and how to split the file, then draws speed and chunk state live.

[中文](README.md) | [English](README.en.md)

- Runs entirely on your machine, no third-party servers
- Zero front-end dependencies: the panel is a single HTML file, the engine pushes SSE events
- Engine and UI are fully decoupled, so either side can be replaced
- Windows (exe / source) and Android tablet (APK) share the same panel and event protocol

## Quick Start

### 1. Run the exe (recommended)

Download `MTAccel.exe` from Releases and double-click it — no Python required. The panel opens in your browser and downloads land in `downloads\` next to the exe.

### 2. Run from source

```bash
git clone <repo-url>
cd MTAccel
pip install -r requirements.txt
python app/server.py
```

On Windows you can also double-click `安装依赖.bat` (install deps) then `启动面板.bat` (start panel).

### 3. Command line

```bash
python app/cli.py "https://example.com/big.iso" -o big.iso
python app/cli.py "https://example.com/big.iso" --threads 32 --limit 10M
python app/cli.py "https://example.com/private.zip" -H "Cookie: session=xxx"
python app/cli.py "https://example.com/big.iso" --hash sha256
```

| Flag | Description |
|---|---|
| `-o, --output` | Output path (inferred from the URL if omitted) |
| `--threads` | Max concurrency (default 32; the engine ramps up from 1 to find the sweet spot) |
| `--chunk` | Force a chunk size (e.g. `8M`; auto-planned by default) |
| `--limit` | Rate limit (e.g. `10M`, `500K`) |
| `-H` | Extra request header, repeatable, `Name: value` |
| `--hash` | Verify the whole file afterwards (`md5` / `sha1` / `sha256`) |

### 4. Android tablet (APK)

Download `MTAccel-android-0.1.0.apk` from Releases and install it. No runtime needed.

- **Requires** Android 7.0 (API 24) or newer
- **Where files go** exported to `Download/MTAccel/` under the system Downloads folder
- **Background** foreground service with notification progress
- **Permissions** network and notifications only

The UI is the same HTML: the app runs a local HTTP server bound to `127.0.0.1` and opens it in a WebView, so the panel is unchanged.

Build from source:

```bash
cd android
# set sdk.dir=/path/to/Android/Sdk in local.properties
gradle assembleRelease
# output: app/build/outputs/apk/release/app-release.apk
```

> If the project path contains non-ASCII characters, Gradle needs `android.overridePathCheck=true` (already set in `gradle.properties`).
> The current release is signed with a debug key for sideloading; use a proper key for store distribution.

## How it works

```
probe → plan → ramp-up → steady state → finish
```

1. **Probe** — `HEAD` for length, then a `Range: bytes=0-65535` probe to confirm the server really supports ranges (many claim to and then return 200 with the whole body). Also measures single-connection baseline speed, ETag and Last-Modified.
2. **Plan** — chunk size derived from file size and max concurrency so the chunk count lands in 8–4096 and no chunk is under 4 MB.
3. **Ramp-up** — concurrency 1 → 2 → 4 → 8 → 16 → 32, observing each level for 2 s and computing marginal gain. Below 15% gain means a bottleneck: step back one level and lock. On 429/503 it backs off immediately and enters a 30 s penalty window.
4. **Steady-state AIMD** — after locking, speed dropping below 85% of the previous level steps down; a stable period triggers a small step up. Near the tail (remaining chunks ≤ concurrency × 2) no more adjustments.
5. **Finish** — verify length, optional whole-file hash, atomic rename from `.part`, delete the resume record.

**Key design**: concurrency is not a fixed split per worker but a **dynamic queue** — workers grab the next chunk as they finish. Uniform chunk sizes mean one slow chunk can't stall the whole job.

## Measured results

Controlled environment (loopback + local mock origin): origin limited to 5 MB/s per connection, 4 connections, 20 MB/s total, 200 MB file.

| Concurrency | Measured | vs previous |
|---|---|---|
| 1 | 4.77 MB/s | — |
| 2 | 8.01 MB/s | +68% |
| 4 | 16.29 MB/s | +103% |
| 8 | 429 triggered (connection cap) | engine fell back to 4 and locked |

Steady state: **200 MB / 13.6 s / 14.7 MB/s average / 19.7 MB/s peak** (98% of the 20 MB/s ceiling).

Real network (python.org, 26.86 MB installer): 1 connection 4.59 MB/s, 2 connections 5.33 MB/s (+16%), total 6.48 s, 4.14 MB/s average.

> Numbers vary with machine, network and origin policy; the table is a same-machine comparison.

## Resume

Pressing `Ctrl+C` (CLI) or "Stop" (panel) leaves a `.part` plus `.part.json`. Starting again with the same output path compares `size / chunk_size / etag / last_modified`:

- All match → only missing chunks are downloaded
- Anything changed → treated as a different file and restarted, never producing a corrupt result

ETag is preferred; Last-Modified is the fallback.

## Layout

```
MTAccel/
├─ core/                  engine (fully decoupled from the UI)
│  ├─ probe.py            HEAD + Range probe, capability detection
│  ├─ planner.py          chunk planning
│  ├─ writer.py           file preallocation, per-worker write handles
│  ├─ resume.py           sidecar resume record (bitstring)
│  ├─ scheduler.py        dynamic concurrency gate + ramp/AIMD controller
│  ├─ engine.py           main loop: workers, retry backoff, event stream
│  └─ verify.py           length check + whole-file hash
├─ app/
│  ├─ cli.py              CLI (rich)
│  ├─ server.py           panel server (aiohttp.web + SSE)
│  └─ web/index.html      single-file front end
├─ tests/                 mock origin, benchmark, engine and panel tests
├─ android/               Android port (Kotlin, standalone Gradle project)
├─ icon.ico
├─ 安装依赖.bat
├─ 启动面板.bat
├─ 下载.bat
└─ requirements.txt
```

**Layering**: `core/` talks only through event dicts; CLI and Web are subscribers. A Rust rewrite of the engine would need no front-end change.

## Known limitations

1. **HTTP(S) direct links only.** No HLS/DASH, torrents, or cloud-drive protocols.
2. **Whole-file hash cannot be parallelised.** Default off; add `--hash` when needed (10 GB costs roughly 10–20 s more).
3. **Signed URLs cannot auto-refresh.** If the origin's ETag changes too, the download restarts — that is the correct behaviour to avoid corrupt output.
4. **Single task.** The panel runs one download at a time; no queue.
5. **Compressed responses fall back to a single stream.** The engine forces `Accept-Encoding: identity`, but some servers ignore it.
6. **Share links that require a login.** Anonymous guest cookies are handled automatically; for real account logins paste a browser `Cookie` header in "Advanced · custom headers".
7. Cloudflare five-second challenges and captchas will fail — that needs a browser, not a downloader.

## Development

```bash
pip install -r requirements.txt

python tests/run_bench.py    # watch every strategy decision
python tests/test_engine.py  # resume / full / rate-limit cases with a built-in origin
python tests/test_web.py     # panel end-to-end in one process
python tests/boot_check.py   # smoke test that the panel starts standalone

python tests/mock_server.py --size 200M --per-conn 5M --conn-limit 4 --total 20M --port 8899
python tests/mock_server.py --no-range --port 8899   # simulate a server without Range support
```

## Packaging

```powershell
pip install pyinstaller
pyinstaller --onefile --name MTAccel --icon icon.ico `
  --add-data "app/web;app/web" app/server.py
```

Output in `dist/`, around 10 MB. The packaged `server.py` points static assets at the PyInstaller extraction directory and downloads next to the exe.

## Roadmap

- ~~Android tablet app (APK)~~ ✅ done (v0.1.0, Kotlin + embedded local server + WebView)
- Android resume support (currently single-shot downloads)
- Multi-task queue and a task list in the panel
- "Strategy comparison" mode: run 1/2/4/8/16 concurrency in one go and plot the results
- Rewrite `core/` in Rust (front end and event protocol unchanged)

## Changelog

See [CHANGELOG.en.md](CHANGELOG.en.md).

## License

[MIT](LICENSE)
