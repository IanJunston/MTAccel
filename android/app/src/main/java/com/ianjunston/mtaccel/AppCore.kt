package com.ianjunston.mtaccel

import android.content.ContentValues
import android.content.Context
import android.os.Build
import android.os.Environment
import android.provider.MediaStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import okhttp3.OkHttpClient
import org.json.JSONObject
import java.io.File
import java.io.OutputStream
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit

/**
 * 应用内核：持有本地 HTTP 服务、下载引擎与全局状态。
 * 面板（assets/index.html）通过 127.0.0.1 上的 HTTP + SSE 与内核通信，
 * 与桌面版 app/server.py 的协议完全一致。
 */
object AppCore {

    private const val DEFAULT_PORT = 8765

    private var server: HttpServer? = null
    private var appCtx: Context? = null
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var job: Job? = null
    private var engine: Engine? = null

    private val clients = CopyOnWriteArrayList<OutputStream>()

    @Volatile
    var port: Int = 0
        private set

    @Volatile
    var status: String = "idle"
        private set

    @Volatile
    var busy: Boolean = false
        private set

    @Volatile
    private var lastProgress: Map<String, Any?> = emptyMap()

    @Volatile
    var lastDone: Map<String, Any?> = emptyMap()
        private set

    private var htmlCache: ByteArray? = null

    /** 跟随匿名登录重定向拿到的 cookie 要留着，飞书分享链接靠它过鉴权。 */
    val cookieJar = MemoryCookieJar()

    private val insecureClient: OkHttpClient by lazy { InsecureClient.build(cookieJar) }

    private val client: OkHttpClient by lazy {
        OkHttpClient.Builder()
            .cookieJar(cookieJar)
            .followRedirects(true)
            .followSslRedirects(true)
            .connectTimeout(15, TimeUnit.SECONDS)
            .readTimeout(60, TimeUnit.SECONDS)
            .writeTimeout(60, TimeUnit.SECONDS)
            .retryOnConnectionFailure(true)
            .build()
    }

    fun downloadDir(ctx: Context): File {
        val base = ctx.getExternalFilesDir(Environment.DIRECTORY_DOWNLOADS) ?: ctx.filesDir
        val dir = File(base, "")
        if (!dir.exists()) dir.mkdirs()
        return dir
    }

    @Synchronized
    fun ensureStarted(ctx: Context): Int {
        appCtx = ctx.applicationContext
        if (port > 0) return port
        val srv = HttpServer(DEFAULT_PORT) { req -> route(req) }
        port = try {
            srv.start()
        } catch (e: Exception) {
            // 端口占用，换一个
            val srv2 = HttpServer(0) { req -> route(req) }
            val p = srv2.start()
            server = srv2
            return p.also { port = it }
        }
        server = srv
        return port
    }

    fun shutdown() {
        runCatching { server?.stop() }
        server = null
        port = 0
    }

    // ---------------- 路由 ----------------

    private fun route(req: HttpReq): HttpResp {
        val ctx = appCtx ?: return HttpResp.text("内核未初始化", 500)
        return when {
            req.method == "GET" && (req.path == "/" || req.path == "/index.html") -> {
                val bytes = htmlCache ?: ctx.assets.open("index.html").use { it.readBytes() }
                    .also { htmlCache = it }
                HttpResp.html(bytes)
            }

            req.method == "GET" && req.path == "/api/events" -> HttpResp.sse { os ->
                clients.add(os)
                try {
                    os.write(": connected\n\n".toByteArray())
                    os.flush()
                    // 补发最近状态
                    if (lastProgress.isNotEmpty()) {
                        os.write("data: ${toJson(lastProgress)}\n\n".toByteArray())
                        os.flush()
                    }
                    while (true) {
                        Thread.sleep(5000)
                        os.write(": ping\n\n".toByteArray())
                        os.flush()
                    }
                } catch (_: Exception) {
                    // 客户端断开
                } finally {
                    clients.remove(os)
                }
            }

            req.method == "GET" && req.path == "/api/state" -> HttpResp.json(
                toJson(
                    mapOf(
                        "status" to status,
                        "busy" to busy,
                        "progress" to lastProgress,
                        "done" to lastDone,
                        "dir" to downloadDir(ctx).absolutePath
                    )
                )
            )

            req.method == "POST" && req.path == "/api/start" -> handleStart(ctx, req)

            req.method == "POST" && req.path == "/api/stop" -> {
                engine?.cancel()
                HttpResp.json("{\"ok\":true}")
            }

            req.method == "POST" && req.path == "/api/config" -> HttpResp.json("{\"ok\":true}")

            req.method == "GET" && req.path == "/favicon.ico" -> HttpResp.text("", 404)

            else -> HttpResp.notFound()
        }
    }

    private fun handleStart(ctx: Context, req: HttpReq): HttpResp {
        if (busy) return HttpResp.text("已有下载任务在进行，请先停止", 409)

        val obj = try {
            JSONObject(req.bodyString().ifBlank { "{}" })
        } catch (e: Exception) {
            return HttpResp.text("请求体必须是 JSON", 400)
        }

        val url = obj.optString("url").trim()
        if (!url.startsWith("http://") && !url.startsWith("https://")) {
            return HttpResp.text("只支持 http/https 直链", 400)
        }

        val dir = downloadDir(ctx)
        val rawOut = obj.optString("output").trim()
        val name = rawOut.ifBlank { guessName(url) }
        val outFile = if (File(name).isAbsolute) File(name) else File(dir, name)

        val maxC = obj.optInt("max_concurrency", 16).coerceIn(1, 32)
        val speedLimit = obj.optDouble("speed_limit", 0.0).let {
            if (it.isNaN() || it <= 0.0) 0L else (it * 1024).toLong()
        }
        val verifyHash = obj.optString("verify_hash").trim().ifBlank { null }
        val algo = obj.optString("verify_algo", "sha256").ifBlank { "sha256" }

        val headers = HashMap<String, String>()
        obj.optJSONObject("headers")?.let { h ->
            h.keys().forEach { k -> headers[k] = h.optString(k) }
        }

        outFile.parentFile?.mkdirs()

        val opts = EngineOptions(
            maxConcurrency = maxC,
            speedLimit = speedLimit,
            verifyHash = verifyHash,
            verifyAlgo = algo,
            autoName = rawOut.isBlank()
        )

        busy = true
        status = "running"
        lastProgress = emptyMap()
        lastDone = emptyMap()

        // 用户勾了「忽略证书校验」就走信任全部的 client，否则用默认的
        val useClient = if (obj.optBoolean("insecure")) insecureClient else client
        val eng = Engine(useClient) { ev -> emit(ev) }
        engine = eng

        job = scope.launch {
            try {
                eng.run(url, outFile, headers, opts)
                // 完成后导出到系统「下载」目录
                val exported = exportToPublic(ctx, outFile)
                // 导出失败（没权限 / 空间不足）也必须发 done，否则前端永远等不到结束
                emit(
                    mapOf(
                        "type" to "done",
                        "size" to outFile.length(),
                        "elapsed" to (lastDone["elapsed"] ?: 0.0),
                        "avg_speed" to (lastDone["avg_speed"] ?: 0L),
                        "peak_speed" to (lastDone["peak_speed"] ?: 0L),
                        "path" to (exported ?: outFile.absolutePath)
                    )
                )
            } catch (e: CancelledException) {
                emit(mapOf("type" to "state", "state" to "cancelled"))
            } catch (e: Exception) {
                emit(mapOf("type" to "error", "message" to (e.message ?: e.javaClass.simpleName)))
                emit(mapOf("type" to "state", "state" to "error"))
            } finally {
                busy = false
                engine = null
            }
        }

        return HttpResp.json("{\"ok\":true}")
    }

    private fun guessName(url: String): String {
        val clean = url.substringBefore('#').substringBefore('?')
        val last = clean.substringAfterLast('/')
        val name = last.ifBlank { "download.bin" }
        return if (name.contains('.')) name else "$name.bin"
    }

    // ---------------- 事件广播 ----------------

    fun emit(ev: Map<String, Any?>) {
        when (ev["type"]) {
            "progress" -> lastProgress = ev
            "state" -> status = (ev["state"] as? String) ?: status
            "done" -> lastDone = ev
        }
        val payload = "data: ${toJson(ev)}\n\n".toByteArray()
        for (os in clients) {
            try {
                os.write(payload)
                os.flush()
            } catch (_: Exception) {
                clients.remove(os)
            }
        }
        DownloadService.onEngineEvent(ev)
    }

    // ---------------- 导出到公共下载目录 ----------------

    private fun exportToPublic(ctx: Context, src: File): String? {
        // Android 10 以下没有分区存储，也没有 MediaStore.Downloads，直接写公共目录
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.Q) {
            return runCatching {
                @Suppress("DEPRECATION")
                val pub = Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_DOWNLOADS)
                val dir = File(pub, "MTAccel").apply { mkdirs() }
                val dst = File(dir, src.name)
                src.inputStream().use { input ->
                    dst.outputStream().use { out -> input.copyTo(out, 256 * 1024) }
                }
                "/Download/MTAccel/${src.name}"
            }.getOrNull()
        }
        return runCatching {
            val values = ContentValues().apply {
                put(MediaStore.Downloads.DISPLAY_NAME, src.name)
                put(MediaStore.Downloads.MIME_TYPE, "application/octet-stream")
                put(MediaStore.Downloads.RELATIVE_PATH, "${Environment.DIRECTORY_DOWNLOADS}/MTAccel")
                put(MediaStore.Downloads.IS_PENDING, 1)
            }
            val resolver = ctx.contentResolver
            val uri = resolver.insert(MediaStore.Downloads.EXTERNAL_CONTENT_URI, values)
                ?: return null
            resolver.openOutputStream(uri)?.use { out ->
                src.inputStream().use { input -> input.copyTo(out, 256 * 1024) }
            }
            values.clear()
            values.put(MediaStore.Downloads.IS_PENDING, 0)
            resolver.update(uri, values, null, null)
            "/Download/MTAccel/${src.name}"
        }.getOrNull()
    }
}
