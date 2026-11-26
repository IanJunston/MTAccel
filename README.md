# MTAccel · 多线程加速下载器

**一个会自动找最佳并发的 HTTP(S) 下载器。** 给一个直链，它先探测服务器能力和当前网络状况，再决定开几条连接、怎么切块，然后边下边把速度和分块状态画出来。

A self-tuning multi-threaded downloader for Windows and Android — probes the server, ramps concurrency to find the sweet spot, and visualizes progress in a local web panel.

- 纯本地运行，不经过任何第三方服务器
- 零前端依赖：面板是单文件 HTML，引擎用 SSE 推事件
- 引擎层与界面层完全解耦，方便替换实现
- Windows（exe / 源码）与 Android 平板（APK）共用同一套面板与事件协议

---

## 快速开始

### 方式一：直接跑 exe（推荐）

从 [Releases](../../releases) 下载 `MTAccel.exe`，双击即可，不需要安装 Python。

浏览器会自动打开面板，下载的文件存到 exe 旁边的 `downloads\` 文件夹。

### 方式二：从源码运行

```bash
git clone <repo-url>
cd MTAccel
pip install -r requirements.txt
python app/server.py
```

Windows 用户也可以双击 `安装依赖.bat`（自动装依赖）→ `启动面板.bat`（启动面板）。

### 方式三：命令行

```bash
python app/cli.py "https://example.com/big.iso" -o big.iso
python app/cli.py "https://example.com/big.iso" --threads 32 --limit 10M
python app/cli.py "https://example.com/private.zip" -H "Cookie: session=xxx"
python app/cli.py "https://example.com/big.iso" --hash sha256
```

| 参数 | 说明 |
|---|---|
| `-o, --output` | 输出路径（省略则从 URL 推断） |
| `--threads` | 最大并发（默认 32，引擎会在 1→32 之间自己爬坡找最佳值） |
| `--chunk` | 强制分块大小（如 `8M`，默认自动规划） |
| `--limit` | 限速（如 `10M`、`500K`） |
| `-H` | 自定义请求头，可重复，格式 `Name: value` |
| `--hash` | 完成后校验整体 hash（`md5`/`sha1`/`sha256`） |

### 方式四：安卓平板（APK）

从 [Releases](../../releases) 下载 `MTAccel-android-0.1.0.apk`，拷到平板安装即可，不需要装任何运行环境。

- **要求**：Android 7.0（API 24）及以上
- **文件存哪**：下载完成后自动导出到系统「下载」目录下的 `Download/MTAccel/`，在文件管理器里直接能看到
- **后台保活**：用前台服务 + 通知栏进度，切到后台也会继续下
- **权限**：只需要网络和通知，不读你的任何文件

界面和桌面版是同一份 HTML：App 内置一个只监听 `127.0.0.1` 的本地 HTTP 服务，WebView 打开它，所以面板一行都没改。

从源码构建：

```bash
cd android
# local.properties 里写 sdk.dir=/path/to/Android/Sdk
gradle assembleRelease
# 产物：app/build/outputs/apk/release/app-release.apk
```

> 项目路径含中文时，Gradle 需要 `android.overridePathCheck=true`（已写进 `gradle.properties`）。
> 当前 release 用 debug 证书签名，方便自装；上架应用商店需换正式签名。

---

## 它会自动做什么

整个流程不需要调参，引擎自己走：

```
预检 → 规划 → 爬坡 → 稳态 → 收尾
```

1. **预检**：HEAD 拿长度，再用 `Range: bytes=0-65535` 探针确认服务器真的支持分块——很多服务器 HEAD 说支持、实际返回 200 全量。同时测出单连接基线速度、ETag、Last-Modified。
2. **规划**：按文件大小和最大并发算分块大小，让块数落在 `8 ~ 4096` 之间、单块不小于 4MB（块太小请求开销会吃掉吞吐）。
3. **爬坡探测**：并发 1 → 2 → 4 → 8 → 16 → 32，每档观察 2 秒算边际增益。增益 < 15% 说明撞到瓶颈，退回上一档锁定。途中遇到 429/503 立刻回退并进入 30 秒惩罚期，不再往上试。
4. **稳态 AIMD**：锁定后继续微调——速度掉到上一档 85% 以下降一档，稳定一段时间后小幅升一档试探。尾部（剩余块数 ≤ 并发×2）不再调档，避免收尾阶段被误判成拥塞。
5. **收尾**：校验长度 → 可选整体 hash → `.part` 原子改名成正式文件 → 删掉续传记录。

**关键设计**：并发不是平均分配给每个 worker 的固定值，而是一个**动态队列**——worker 抢块，谁先完成谁抢下一块。块大小均匀，所以不会出现「某个 worker 拿到慢块拖死整个任务」的长尾。

---

## 实测数据

### 受控环境（本机环回 + 自建 mock 源站）

源站限制：单连接 5MB/s、连接数上限 4、总带宽上限 20MB/s。文件 200MB。

| 并发 | 实测速度 | 相对上一档 |
|---|---|---|
| 1 | 4.77 MB/s | — |
| 2 | 8.01 MB/s | +68% |
| 4 | 16.29 MB/s | +103% |
| 8 | 触发 429（超过连接数上限） | 引擎自动回退到 4 并锁定 |

稳态成绩：**200MB / 13.6s / 平均 14.7 MB/s / 峰值 19.7 MB/s**（理论上限 20 MB/s，达成 98%）。

### 真实外网（python.org，26.86 MB 安装包）

| 并发 | 实测速度 | 边际增益 |
|---|---|---|
| 1 | 4.59 MB/s | — |
| 2 | 5.33 MB/s | +16%（刚过 15% 阈值，继续爬） |

全程 6.48s，平均 4.14 MB/s，峰值 5.63 MB/s。下载完的文件 `MZ` 头正确、长度与 `Content-Length` 一致。

### 其他验证项

| 场景 | 结果 |
|---|---|
| 限速 10MB/s | 实测 9.86 MB/s（令牌桶） |
| 中断后续传 | 已下部分不重下，最终 hash 与源一致 |
| 服务器忽略 Range | 自动退化为单流，30MB / 0.38s |
| 面板端到端 | 200MB / 13.56s / 14.75 MB/s，9 类事件齐全 |

> 数值随机器、网络、源站策略波动，上表是同机同条件下的对比。

---

## 断点续传

下载中随时按 `Ctrl+C`（CLI）或点「停止」（面板），会落一个 `.part` + `.part.json`。
再次对同一个输出路径启动，会比对 `size / chunk_size / etag / last_modified` 四项：

- 全部匹配 → 只下没完成的块，`已下载` 从断点继续
- 任何一项变了（源文件更新了）→ 判定为不同文件，从头开始，不会拼出一个坏文件

ETag 优先，没有 ETag 才用 Last-Modified 兜底。

---

## 目录结构

```
MTAccel/
├─ core/                 引擎层（与界面完全解耦）
│  ├─ probe.py           预检：HEAD + Range 探针、能力识别
│  ├─ planner.py         分块规划
│  ├─ writer.py          预分配文件、每个 worker 独立写句柄
│  ├─ resume.py          sidecar 续传记录（位串压缩）
│  ├─ scheduler.py       动态并发闸门 + 爬坡/AIMD 控制器
│  ├─ engine.py          主循环：worker、重试退避、事件流
│  └─ verify.py          长度校验 + 整体 hash
├─ app/
│  ├─ cli.py             命令行界面（rich）
│  ├─ server.py          面板服务（aiohttp.web + SSE）
│  └─ web/index.html     单文件前端（零依赖）
├─ tests/
│  ├─ mock_server.py     可配速/配连接数/可关 Range 的模拟源站
│  ├─ run_bench.py       直接驱动引擎打印策略决策
│  ├─ test_engine.py     续传 / 全量 / 限速 三用例（自包含）
│  ├─ test_web.py        自包含端到端（同进程起源站+面板）
│  └─ boot_check.py      冒烟检查：确认面板能独立启动
├─ android/              安卓版（Kotlin，独立 Gradle 工程）
│  └─ app/src/main/
│     ├─ java/com/ianjunston/mtaccel/
│     │  ├─ Engine.kt        引擎：探测/分块/并发/重试/校验（OkHttp + 协程）
│     │  ├─ HttpServer.kt    极简 HTTP 服务 + SSE + JSON 序列化
│     │  ├─ AppCore.kt       路由、状态、事件广播、导出到系统下载目录
│     │  ├─ MainActivity.kt  WebView 加载 127.0.0.1 面板
│     │  └─ DownloadService.kt 前台服务 + 通知栏进度
│     └─ assets/index.html   与桌面版同一份面板
├─ icon.ico
├─ 安装依赖.bat
├─ 启动面板.bat
├─ 下载.bat
└─ requirements.txt
```

**分层原则**：`core/` 只通过事件字典对外说话，CLI 和 Web 都是订阅者。所以以后用 Rust 重写引擎时，前端一行都不用改。

### 事件协议

| type | 关键字段 | 含义 |
|---|---|---|
| `probe` | size / accept_ranges / ttfb / single_speed / note | 预检结果 |
| `plan` | chunk_size / total_chunks / resumed | 分块方案 |
| `chunk` | i / s（`started`/`done`/`failed`） | 单块状态（画热力图用） |
| `strategy` | level / speed / reason / phase | 策略决策（爬坡/AIMD 每一步） |
| `progress` | done_bytes / total / speed / active / errors | 进度心跳（默认 200ms） |
| `verify` | stage / ok / detail | 校验进度 |
| `state` | state（`running`/`paused`/`done`/`error`） | 生命周期 |
| `error` | msg | 致命错误 |
| `done` | path / bytes / seconds / avg_speed / peak_speed | 完成汇总 |

---

## 已知限制

1. **只支持 HTTP(S) 直链**。不支持 HLS/DASH 切片流、BT/磁力、网盘需要走客户端协议的资源。
2. **整体 hash 无法并行**。hash 是链式依赖，必须顺序读一遍。10GB 文件约多花 10~20 秒，所以默认关闭，需要时加 `--hash`。
3. **签名 URL 过期后不能自动刷新**。链接过期需要重新提供；如果源站 ETag 也变了，引擎会判定为新文件从头下载（这是正确行为，避免拼坏文件）。
4. **单任务模型**。面板同时只跑一个下载，没有队列。
5. **服务器返回压缩编码时会退化为单流**。引擎强制 `Accept-Encoding: identity` 来保证字节偏移正确，但少数服务器无视这个头，此时只能单流跑。
6. 遇到 Cloudflare 五秒盾、验证码类站点会失败——那需要浏览器行为，不是下载器能解决的。

---

## 开发与测试

```bash
pip install -r requirements.txt

# 策略调试：直接看引擎每一档的决策
python tests/run_bench.py

# 引擎三用例（续传 / 全量 / 限速，自带源站）
python tests/test_engine.py

# 面板端到端（同进程起 mock 源站 + 面板）
python tests/test_web.py

# 冒烟：确认 python app/server.py 能独立起服务
python tests/boot_check.py

# 单独起一个模拟源站手动测
python tests/mock_server.py --size 200M --per-conn 5M --conn-limit 4 --total 20M --port 8899
python tests/mock_server.py --no-range --port 8899    # 模拟不支持 Range 的服务器
```

## 打包 exe

```powershell
pip install pyinstaller

# Windows（--add-data 用分号分隔）
pyinstaller --onefile --name MTAccel --icon icon.ico `
  --add-data "app/web;app/web" app/server.py
```

```bash
# Linux / macOS（--add-data 用冒号分隔）
pyinstaller --onefile --name MTAccel --icon icon.ico \
  --add-data "app/web:app/web" app/server.py
```

产物在 `dist/`，约 10MB。打包后 `server.py` 会自动把静态资源指向 PyInstaller 的解压目录、把默认下载目录指到 exe 旁边。

---

## Roadmap

- ~~Android 平板版（APK）~~ ✅ 已完成（v0.1.0，Kotlin + 内嵌本地服务 + WebView 复用面板）
- 安卓端断点续传（当前只支持单次完整下载）
- 多任务队列 + 面板任务列表
- 「策略对比实验」模式：一次跑 1/2/4/8/16 并发，自动出对比图
- 用 Rust 重写 `core/`（前端与事件协议不变，直接替换）

## License

[MIT](LICENSE)
