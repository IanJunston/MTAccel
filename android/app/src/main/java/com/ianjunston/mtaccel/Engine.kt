package com.ianjunston.mtaccel

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.File
import java.io.RandomAccessFile
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max
import kotlin.math.min

typealias Emit = (Map<String, Any?>) -> Unit

data class EngineOptions(
    val maxConcurrency: Int = 16,
    val speedLimit: Long = 0L,
    val verifyHash: String? = null,
    val verifyAlgo: String = "sha256",
    val autoName: Boolean = false
)

/**
 * 一个下载单元。
 * endIncl < 0 表示「长度未知，读到 EOF 为止」——此时不带 Range，也不预分配。
 */
data class ChunkSpec(val index: Int, val start: Long, val endIncl: Long) {
    val length: Long get() = endIncl - start + 1
    val unlimited: Boolean get() = endIncl < 0
}

data class ProbeResult(
    val size: Long,                       // <=0 表示服务器没给长度
    val acceptRanges: Boolean,
    val etag: String?,
    val singleSpeed: Long,
    val filename: String? = null,
    val contentType: String? = null,
    val needsAuth: Boolean = false,
    val note: String = ""
)

class CancelledException : Exception("已取消")
class RateLimitedException(val waitMs: Long) : Exception("429 限流")

/** 令牌桶限速，rate 为 0 表示不限速 */
class TokenBucket(private val rate: Long) {
    private var tokens = 0.0
    private var last = System.nanoTime()
    private val lock = Object()

    fun take(n: Long) {
        if (rate <= 0L || n <= 0L) return
        synchronized(lock) {
            while (true) {
                val now = System.nanoTime()
                tokens = min(rate.toDouble(), tokens + (now - last) / 1e9 * rate)
                last = now
                if (tokens >= n) {
                    tokens -= n
                    return
                }
                val waitMs = ((n - tokens) / rate * 1000.0).toLong().coerceIn(1L, 200L)
                try {
                    lock.wait(waitMs)
                } catch (_: InterruptedException) {
                    return
                }
            }
        }
    }
}

/** 默认用浏览器 UA：不少国内 CDN（飞书、蓝奏等）会直接 401 掉 okhttp/x.y.z */
private const val DEFAULT_UA =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

private val LOGIN_HINTS = listOf(
    "passport", "/login", "signin", "sign_in", "/sso/", "oauth",
    "auth/login", "login.microsoftonline", "accounts.google.com", "login.live.com"
)

private fun looksLikeLogin(u: String): Boolean {
    val ul = u.lowercase()
    return LOGIN_HINTS.any { ul.contains(it) }
}

private fun parseFilename(cd: String?): String? {
    if (cd.isNullOrBlank()) return null
    Regex("filename\\*\\s*=\\s*([^;]+)", RegexOption.IGNORE_CASE).find(cd)?.let { m ->
        var v = m.groupValues[1].trim().trim('"')
        if (v.contains("''")) {
            val idx = v.indexOf("''")
            val cs = v.substring(0, idx).ifBlank { "utf-8" }
            v = v.substring(idx + 2)
            return runCatching { java.net.URLDecoder.decode(v, cs) }.getOrElse { v }
        }
        return runCatching { java.net.URLDecoder.decode(v, "utf-8") }.getOrElse { v }
    }
    Regex("filename\\s*=\\s*\"?([^\";]+)\"?", RegexOption.IGNORE_CASE).find(cd)?.let { m ->
        val raw = m.groupValues[1].trim()
        // 很多服务端把 UTF-8 字节直接塞进 latin-1 头里
        return runCatching { String(raw.toByteArray(Charsets.ISO_8859_1), Charsets.UTF_8) }
            .getOrElse { raw }
    }
    return null
}

class Engine(
    private val client: OkHttpClient,
    private val emit: Emit
) {
    @Volatile
    var cancelled: Boolean = false
        private set

    fun cancel() {
        cancelled = true
    }

    // ---------------- 探测 ----------------

    suspend fun probe(url: String, headers: Map<String, String>): ProbeResult =
        withContext(Dispatchers.IO) {
            var size = -1L
            var ranges = false
            var etag: String? = null
            var ct: String? = null
            var fname: String? = null
            var speed = 0L
            var status = 0
            var finalUrl = url
            val notes = ArrayList<String>()

            // 1) HEAD —— 不少 CDN 对 HEAD 直接 404/405，失败不致命
            runCatching {
                val req = Request.Builder().url(url).head()
                    .header("User-Agent", DEFAULT_UA)
                .header("Accept", "*/*")
                    .apply { headers.forEach { (k, v) -> header(k, v) } }
                    .build()
                client.newCall(req).execute().use { r ->
                    status = r.code
                    finalUrl = r.request.url.toString()
                    if (r.isSuccessful) {
                        size = r.header("Content-Length")?.toLongOrNull() ?: -1L
                        ranges = r.header("Accept-Ranges")?.contains("bytes", true) == true
                        etag = r.header("ETag")
                        ct = r.header("Content-Type")
                        fname = parseFilename(r.header("Content-Disposition"))
                    } else {
                        notes.add("HEAD ${r.code}")
                    }
                }
            }.onFailure { notes.add("HEAD 失败:${it.javaClass.simpleName}") }

            // 2) Range GET 256KB —— 一次同时拿到长度、能力、文件名和基线速度
            val probeLen = 256L * 1024 - 1
            runCatching {
                val req = Request.Builder().url(url)
                    .header("Range", "bytes=0-$probeLen")
                    .header("Accept-Encoding", "identity")
                    .header("User-Agent", DEFAULT_UA)
                .header("Accept", "*/*")
                    .apply { headers.forEach { (k, v) -> header(k, v) } }
                    .build()
                val t0 = System.currentTimeMillis()
                client.newCall(req).execute().use { r ->
                    status = r.code
                    finalUrl = r.request.url.toString()
                    ct = ct ?: r.header("Content-Type")
                    fname = fname ?: parseFilename(r.header("Content-Disposition"))
                    etag = etag ?: r.header("ETag")
                    if (r.code == 206) {
                        ranges = true
                        Regex("/(\\d+)\\s*$").find(r.header("Content-Range") ?: "")?.let {
                            size = it.groupValues[1].toLong()
                        }
                        val body = r.body
                        if (body != null) {
                            val input = body.byteStream()
                            val buf = ByteArray(64 * 1024)
                            var total = 0L
                            while (total <= probeLen) {
                                val want = min(buf.size.toLong(), probeLen - total + 1).toInt()
                                val n = input.read(buf, 0, want)
                                if (n < 0) break
                                total += n
                            }
                            val dt = (System.currentTimeMillis() - t0) / 1000.0
                            if (dt > 0.003 && total > 0) { speed = (total / dt).toLong() }
                        }
                        Unit
                    } else if (r.code == 200) {
                        ranges = false
                        if (size <= 0L) size = r.header("Content-Length")?.toLongOrNull() ?: -1L
                        notes.add("服务器忽略 Range")
                    } else {
                        notes.add("Range 探测 HTTP ${r.code}")
                    }
                }
            }.onFailure { notes.add("Range 探测失败:${it.javaClass.simpleName}") }

            // 3) 诊断：拿不到长度时说清原因，能下就照下，不能下给可执行的解法
            var needsAuth = false
            val ctLower = (ct ?: "").lowercase()
            val isHtml = ctLower.startsWith("text/html") || ctLower.startsWith("application/xhtml")
            val note: String

            if (looksLikeLogin(finalUrl)) {
                // 登录页的 Content-Length 是网页大小，必须作废
                size = -1L
                ranges = false
                needsAuth = true
                note = "链接需要登录：请求被重定向到了登录页。浏览器能下是因为带着你的登录 Cookie。" +
                    "请按 F12 → Network 找到该请求 → Copy as cURL，把 Cookie 整行填到「高级 · 自定义请求头」里再试。"
            } else if (status == 401 || status == 403) {
                needsAuth = true
                note = "无权限（HTTP $status）：链接可能需要 Cookie / Referer，或签名已过期。" +
                    "可在「高级 · 自定义请求头」里补上再试。"
            } else if (size > 0L) {
                note = if (ranges) "支持分块下载" else "服务器不支持 Range，单流模式"
            } else if (isHtml) {
                note = "返回的是网页而不是文件：这个地址多半是分享页/详情页，不是文件直链。"
            } else if (status >= 400) {
                note = "服务器返回 HTTP $status，拿不到文件。${if (notes.isEmpty()) "" else "（" + notes.joinToString("；") + "）"}"
            } else {
                note = "服务器没给文件长度，将按未知大小单流下载。" +
                    if (notes.isEmpty()) "" else "（" + notes.joinToString("；") + "）"
            }

            ProbeResult(
                size = max(size, -1L),
                acceptRanges = ranges,
                etag = etag,
                singleSpeed = speed,
                filename = fname,
                contentType = ct,
                needsAuth = needsAuth,
                note = note
            )
        }

    // ---------------- 分块规划 ----------------

    private fun planChunks(size: Long): List<ChunkSpec> {
        val minChunk = 4L * 1024 * 1024
        val maxChunk = 64L * 1024 * 1024
        val maxChunks = 4096
        var cs = min(maxChunk, max(minChunk, size / 64))
        while (size / cs > maxChunks && cs < maxChunk) cs *= 2
        if (size <= minChunk) cs = size.coerceAtLeast(1)
        val out = ArrayList<ChunkSpec>()
        var pos = 0L
        var i = 0
        while (pos < size) {
            val end = min(size - 1, pos + cs - 1)
            out.add(ChunkSpec(i++, pos, end))
            pos = end + 1
        }
        return out
    }

    // ---------------- 主流程 ----------------

    suspend fun run(
        url: String,
        output: File,
        headers: Map<String, String>,
        opts: EngineOptions
    ) = coroutineScope {
        cancelled = false
        val t0 = System.currentTimeMillis()
        emit(mapOf("type" to "state", "state" to "probing"))

        val probe = probe(url, headers)
        if (probe.needsAuth) throw RuntimeException(probe.note)

        // 服务器在 Content-Disposition 里给了真名就用它，比从 URL 猜准得多
        var outFile = output
        if (opts.autoName) {
            probe.filename?.let { fn ->
                val safe = fn.map { ch ->
                    if (ch.isLetterOrDigit() || ch in "._-()+ []（）") ch else '_'
                }.joinToString("").trim().take(120)
                if (safe.isNotBlank()) outFile = File(output.parentFile, safe)
            }
        }

        emit(
            mapOf(
                "type" to "probe",
                "size" to max(probe.size, 0L),
                "accept_ranges" to probe.acceptRanges,
                "single_speed" to probe.singleSpeed,
                "etag" to probe.etag,
                "filename" to probe.filename,
                "note" to probe.note
            )
        )

        val doneBytes = AtomicLong(0)
        val errors = AtomicInteger(0)
        val peak = AtomicLong(0)
        val activeWorkers = AtomicInteger(0)
        val bucket = TokenBucket(opts.speedLimit)

        val unknown = probe.size <= 0L
        val singleMode = unknown || !probe.acceptRanges || probe.size < 4L * 1024 * 1024
        val chunks: List<ChunkSpec> = when {
            unknown -> listOf(ChunkSpec(0, 0L, -1L))
            singleMode -> listOf(ChunkSpec(0, 0L, probe.size - 1))
            else -> planChunks(probe.size)
        }

        emit(
            mapOf(
                "type" to "plan",
                "mode" to if (singleMode) "single" else "parallel",
                "size" to max(probe.size, 0L),
                "chunk_size" to (if (unknown) 0L else chunks.first().length),
                "total_chunks" to chunks.size,
                "resumed" to false,
                "done_chunks" to 0
            )
        )

        // 只有知道长度才预分配；未知长度边下边涨
        RandomAccessFile(outFile, "rw").use { raf ->
            if (!unknown) raf.setLength(probe.size)
        }

        val queue = Channel<ChunkSpec>(chunks.size)
        chunks.forEach { queue.trySend(it) }
        queue.close()

        val workerCount = if (singleMode) 1 else opts.maxConcurrency
        val gateLimit = AtomicInteger(if (singleMode) 1 else min(4, workerCount))

        emit(mapOf("type" to "state", "state" to "running"))

        // 进度播报
        val progressJob = launch {
            var lastBytes = 0L
            var lastT = System.currentTimeMillis()
            var runningPeak = 0L
            while (isActive) {
                delay(250)
                val now = System.currentTimeMillis()
                val cur = doneBytes.get()
                val dt = (now - lastT) / 1000.0
                val speed = if (dt > 0) ((cur - lastBytes) / dt).toLong() else 0L
                if (speed > runningPeak) {
                    runningPeak = speed
                    peak.set(speed)
                }
                val elapsed = (now - t0) / 1000.0
                val avg = if (elapsed > 0) (cur / elapsed).toLong() else 0L
                emit(
                    mapOf(
                        "type" to "progress",
                        "total" to max(probe.size, 0L),
                        "confirmed" to cur,
                        "percent" to (if (probe.size > 0) (cur * 100.0 / probe.size) else 0.0),
                        "speed" to speed,
                        "eta" to (if (probe.size > 0 && speed > 0) ((probe.size - cur) / speed) else 0L),
                        "concurrency" to gateLimit.get(),
                        "active" to activeWorkers.get(),
                        "avg_speed" to avg,
                        "errors" to errors.get()
                    )
                )
                lastBytes = cur
                lastT = now
                if (probe.size > 0 && cur >= probe.size) break
            }
        }

        // 策略控制器：先爬坡找上限，再 AIMD 微调
        val strategyJob = launch {
            var best = 0.0
            var cur = gateLimit.get()
            var phase = "ramp"
            var lastTotal = 0L
            var lastT = System.currentTimeMillis()
            while (isActive) {
                delay(2000)
                val now = System.currentTimeMillis()
                val dt = (now - lastT) / 1000.0
                val tot = doneBytes.get()
                val sp = if (dt > 0) ((tot - lastTotal) / dt) else 0.0
                lastTotal = tot
                lastT = now
                if (sp > best) best = sp

                if (phase == "ramp") {
                    val gain = if (best > 0) (sp - best) / best else 1.0
                    if (cur < opts.maxConcurrency && (best <= 0.0 || gain > -0.15)) {
                        cur = min(opts.maxConcurrency, cur * 2)
                        gateLimit.set(cur)
                        emit(
                            mapOf(
                                "type" to "strategy",
                                "phase" to "ramp",
                                "concurrency" to cur,
                                "speed" to sp.toLong(),
                                "gain" to gain,
                                "reason" to "爬坡探测"
                            )
                        )
                    } else {
                        phase = "aimd"
                        emit(
                            mapOf(
                                "type" to "strategy",
                                "phase" to "aimd",
                                "concurrency" to cur,
                                "speed" to sp.toLong(),
                                "gain" to 0.0,
                                "reason" to "收敛锁定"
                            )
                        )
                    }
                } else {
                    if (cur < opts.maxConcurrency && sp >= best * 0.95) {
                        cur = min(opts.maxConcurrency, cur + 1)
                        gateLimit.set(cur)
                        emit(
                            mapOf(
                                "type" to "strategy",
                                "phase" to "aimd",
                                "concurrency" to cur,
                                "speed" to sp.toLong(),
                                "gain" to (if (best > 0) (sp - best) / best else 0.0),
                                "reason" to "稳态试探 +1"
                            )
                        )
                    }
                }
                if (probe.size > 0 && doneBytes.get() >= probe.size) break
            }
        }

        val workers = ArrayList<kotlinx.coroutines.Job>()
        for (id in 0 until workerCount) {
            workers.add(launch(Dispatchers.IO) {
                val raf = RandomAccessFile(outFile, "rw")
                try {
                    while (true) {
                        if (cancelled) throw CancelledException()
                        if (id >= gateLimit.get()) {
                            delay(120)
                            continue
                        }
                        val r = queue.tryReceive()
                        val c = r.getOrNull()
                        if (c == null) break
                        activeWorkers.incrementAndGet()
                        emit(mapOf("type" to "chunk", "i" to c.index, "s" to "active"))
                        try {
                            fetchChunk(url, headers, c, raf, bucket, doneBytes)
                            emit(mapOf("type" to "chunk", "i" to c.index, "s" to "done"))
                        } catch (e: CancelledException) {
                            throw e
                        } catch (e: Exception) {
                            errors.incrementAndGet()
                            emit(mapOf("type" to "chunk", "i" to c.index, "s" to "failed"))
                            // 未知长度的流不能重投，否则会重复计数
                            if (!c.unlimited) queue.trySend(c)
                            if (errors.get() > chunks.size * 3) throw e
                        } finally {
                            activeWorkers.decrementAndGet()
                        }
                    }
                } finally {
                    runCatching { raf.close() }
                }
            })
        }

        try {
            workers.forEach { it.join() }
        } finally {
            progressJob.cancel()
            strategyJob.cancel()
        }

        if (cancelled) {
            emit(mapOf("type" to "state", "state" to "cancelled"))
            throw CancelledException()
        }

        // 校验
        val actual = outFile.length()
        if (probe.size > 0 && actual != probe.size) {
            emit(mapOf("type" to "error", "message" to "文件大小不符：期望 ${probe.size}，实际 $actual"))
            throw RuntimeException("文件大小校验失败")
        }
        emit(mapOf("type" to "verify", "stage" to "size", "ok" to true))

        opts.verifyHash?.let { want ->
            emit(mapOf("type" to "verify", "stage" to "hash", "percent" to 0.0))
            val got = hashFile(outFile, opts.verifyAlgo) { pct ->
                emit(mapOf("type" to "verify", "stage" to "hash", "percent" to pct))
            }
            if (!got.equals(want, ignoreCase = true)) {
                emit(mapOf("type" to "error", "message" to "Hash 校验不匹配"))
                throw RuntimeException("Hash 校验失败")
            }
        }

        val realSize = if (probe.size > 0) probe.size else actual
        val elapsed = (System.currentTimeMillis() - t0) / 1000.0
        emit(
            mapOf(
                "type" to "done",
                "size" to realSize,
                "elapsed" to elapsed,
                "avg_speed" to (if (elapsed > 0) (realSize / elapsed).toLong() else 0L),
                "peak_speed" to peak.get(),
                "path" to outFile.absolutePath
            )
        )
        emit(mapOf("type" to "state", "state" to "done"))
    }

    private suspend fun fetchChunk(
        url: String,
        headers: Map<String, String>,
        c: ChunkSpec,
        raf: RandomAccessFile,
        bucket: TokenBucket,
        doneBytes: AtomicLong
    ) = withContext(Dispatchers.IO) {
        var attempt = 0
        val maxAttempt = if (c.unlimited) 1 else 4
        while (attempt < maxAttempt) {
            if (cancelled) throw CancelledException()
            val rb = Request.Builder().url(url)
                .header("Accept-Encoding", "identity")
                .header("User-Agent", DEFAULT_UA)
                .header("Accept", "*/*")
                .apply { headers.forEach { (k, v) -> header(k, v) } }
            if (!c.unlimited) rb.header("Range", "bytes=${c.start}-${c.endIncl}")
            val req = rb.build()
            try {
                client.newCall(req).execute().use { resp ->
                    if (resp.code == 429 || resp.code == 503) {
                        throw RateLimitedException(
                            (resp.header("Retry-After")?.toLongOrNull() ?: 2L) * 1000L
                        )
                    }
                    if (resp.code == 416) {
                        if (!c.unlimited) doneBytes.addAndGet(c.length)
                        return@withContext
                    }
                    if (resp.code !in 200..299) throw RuntimeException("HTTP ${resp.code}")
                    val body = resp.body ?: throw RuntimeException("空响应体")
                    val input = body.byteStream()
                    raf.seek(c.start)
                    val buf = ByteArray(64 * 1024)
                    var pos = c.start
                    while (c.unlimited || pos <= c.endIncl) {
                        if (cancelled) throw CancelledException()
                        val want = if (c.unlimited) buf.size
                        else min(buf.size.toLong(), c.endIncl - pos + 1).toInt()
                        val n = input.read(buf, 0, want)
                        if (n < 0) break
                        raf.write(buf, 0, n)
                        pos += n
                        doneBytes.addAndGet(n.toLong())
                        bucket.take(n.toLong())
                    }
                    if (!c.unlimited && pos <= c.endIncl) throw RuntimeException("连接提前中断")
                    return@withContext
                }
            } catch (e: RateLimitedException) {
                attempt++
                delay(e.waitMs.coerceIn(1000L, 8000L))
            } catch (e: CancelledException) {
                throw e
            } catch (e: Exception) {
                attempt++
                if (attempt >= maxAttempt) throw e
                delay(300L * attempt)
            }
        }
        throw RuntimeException("分块 ${c.index} 重试耗尽")
    }

    private suspend fun hashFile(file: File, algo: String, onProgress: (Double) -> Unit): String =
        withContext(Dispatchers.IO) {
            val md = MessageDigest.getInstance(if (algo.equals("md5", true)) "MD5" else "SHA-256")
            val total = file.length().coerceAtLeast(1L)
            var read = 0L
            file.inputStream().use { input ->
                val buf = ByteArray(256 * 1024)
                var lastPct = -1
                while (true) {
                    val n = input.read(buf)
                    if (n < 0) break
                    md.update(buf, 0, n)
                    read += n
                    val pct = (read * 100.0 / total).toInt()
                    if (pct != lastPct) {
                        lastPct = pct
                        onProgress(read * 100.0 / total)
                    }
                }
            }
            md.digest().joinToString("") { "%02x".format(it) }
        }
}
