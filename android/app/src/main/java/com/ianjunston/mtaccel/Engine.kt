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
    val verifyAlgo: String = "sha256"
)

data class ChunkSpec(val index: Int, val start: Long, val endIncl: Long) {
    val length: Long get() = endIncl - start + 1
}

data class ProbeResult(
    val size: Long,
    val acceptRanges: Boolean,
    val etag: String?,
    val singleSpeed: Long
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

            runCatching {
                val req = Request.Builder().url(url).head().apply {
                    headers.forEach { (k, v) -> header(k, v) }
                }.build()
                client.newCall(req).execute().use { r ->
                    if (r.isSuccessful) {
                        size = r.header("Content-Length")?.toLongOrNull() ?: -1L
                        ranges = r.header("Accept-Ranges")?.contains("bytes", true) == true
                        etag = r.header("ETag")
                    }
                }
            }

            if (size <= 0L || !ranges) {
                runCatching {
                    val req = Request.Builder().url(url)
                        .header("Range", "bytes=0-0")
                        .header("Accept-Encoding", "identity")
                        .apply { headers.forEach { (k, v) -> header(k, v) } }
                        .build()
                    client.newCall(req).execute().use { r ->
                        val cr = r.header("Content-Range")
                        if (r.code == 206 && cr != null) {
                            ranges = true
                            Regex("/(\\d+)\\s*$").find(cr)?.let { size = it.groupValues[1].toLong() }
                            if (etag == null) etag = r.header("ETag")
                        } else if (r.code == 200) {
                            ranges = false
                            size = r.header("Content-Length")?.toLongOrNull() ?: size
                        }
                    }
                }
            }

            // 单连接测速：拉 1MB 计时
            var single = 0L
            runCatching {
                val probeLen = if (size in 1..(2L * 1024 * 1024)) size - 1 else 1024L * 1024 - 1
                val req = Request.Builder().url(url)
                    .header("Range", "bytes=0-$probeLen")
                    .header("Accept-Encoding", "identity")
                    .apply { headers.forEach { (k, v) -> header(k, v) } }
                    .build()
                val t0 = System.currentTimeMillis()
                client.newCall(req).execute().use { r ->
                    if (r.isSuccessful) {
                        val body = r.body
                        if (body != null) {
                            val buf = ByteArray(64 * 1024)
                            val input = body.byteStream()
                            var total = 0L
                            while (total < probeLen) {
                                val n = input.read(buf, 0, min(buf.size.toLong(), probeLen - total + 1).toInt())
                                if (n < 0) break
                                total += n
                            }
                            val dt = (System.currentTimeMillis() - t0) / 1000.0
                            if (dt > 0.003 && total > 0) single = (total / dt).toLong()
                        }
                    }
                }
            }

            ProbeResult(
                size = max(size, 0L),
                acceptRanges = ranges,
                etag = etag,
                singleSpeed = single
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
        if (probe.size <= 0L) throw RuntimeException("无法获取文件大小，链接可能无效")

        emit(
            mapOf(
                "type" to "probe",
                "size" to probe.size,
                "accept_ranges" to probe.acceptRanges,
                "single_speed" to probe.singleSpeed,
                "etag" to probe.etag,
                "note" to if (probe.acceptRanges) "支持分块下载" else "服务器不支持 Range，单流模式"
            )
        )

        val doneBytes = AtomicLong(0)
        val errors = AtomicInteger(0)
        val peak = AtomicLong(0)
        val activeWorkers = AtomicInteger(0)
        val bucket = TokenBucket(opts.speedLimit)

        val singleMode = !probe.acceptRanges || probe.size < 4L * 1024 * 1024

        val chunks: List<ChunkSpec> = if (singleMode) {
            listOf(ChunkSpec(0, 0L, probe.size - 1))
        } else {
            planChunks(probe.size)
        }

        emit(
            mapOf(
                "type" to "plan",
                "mode" to if (singleMode) "single" else "parallel",
                "size" to probe.size,
                "chunk_size" to chunks.first().length,
                "total_chunks" to chunks.size,
                "resumed" to false,
                "done_chunks" to 0
            )
        )

        RandomAccessFile(output, "rw").use { it.setLength(probe.size) }

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
                val avg = if (elapsed > 0.2) (cur / elapsed).toLong() else 0L
                val remain = (probe.size - cur).coerceAtLeast(0L)
                val eta = if (speed > 0) (remain / speed).toInt() else -1
                emit(
                    mapOf(
                        "type" to "progress",
                        "total" to probe.size,
                        "confirmed" to cur,
                        "percent" to (if (probe.size > 0) cur * 100.0 / probe.size else 0.0),
                        "speed" to speed,
                        "eta" to eta,
                        "concurrency" to gateLimit.get(),
                        "active" to activeWorkers.get(),
                        "avg_speed" to avg,
                        "errors" to errors.get()
                    )
                )
                lastBytes = cur
                lastT = now
                if (cur >= probe.size) break
            }
        }

        // 策略控制器
        val strategyJob = launch {
            if (singleMode) return@launch
            var best = 0L
            var phase = "ramp"
            var cur = gateLimit.get()
            var waited = 0
            while (isActive) {
                delay(2000)
                waited += 2
                val sp = peak.get()
                if (phase == "ramp") {
                    val gain = if (best > 0) (sp - best).toDouble() / best else 1.0
                    if (sp > best) best = sp
                    if (cur < workerCount && (best == 0L || gain > 0.15)) {
                        cur = min(workerCount, cur * 2)
                        gateLimit.set(cur)
                        emit(
                            mapOf(
                                "type" to "strategy",
                                "phase" to "ramp",
                                "concurrency" to cur,
                                "speed" to sp,
                                "gain" to gain,
                                "reason" to "爬坡增益 ${(gain * 100).toInt()}%"
                            )
                        )
                    } else {
                        phase = "aimd"
                        emit(
                            mapOf(
                                "type" to "strategy",
                                "phase" to "aimd",
                                "concurrency" to cur,
                                "speed" to sp,
                                "gain" to (if (best > 0) (sp - best).toDouble() / best else 0.0),
                                "reason" to "边际增益不足，锁定并发"
                            )
                        )
                    }
                } else {
                    if (waited % 6 == 0 && cur < workerCount && sp >= best * 95 / 100) {
                        cur += 1
                        gateLimit.set(cur)
                        emit(
                            mapOf(
                                "type" to "strategy",
                                "phase" to "aimd",
                                "concurrency" to cur,
                                "speed" to sp,
                                "gain" to (if (best > 0) (sp - best).toDouble() / best else 0.0),
                                "reason" to "稳态试探 +1"
                            )
                        )
                    }
                }
                if (doneBytes.get() >= probe.size) break
            }
        }

        val workers = ArrayList<kotlinx.coroutines.Job>()
        for (id in 0 until workerCount) {
            workers.add(launch(Dispatchers.IO) {
                val raf = RandomAccessFile(output, "rw")
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
                            // 失败重投一次
                            queue.trySend(c)
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
        emit(mapOf("type" to "verify", "stage" to "size", "ok" to true))
        val actual = output.length()
        if (actual != probe.size) {
            emit(mapOf("type" to "error", "message" to "文件大小不符：期望 ${probe.size}，实际 $actual"))
            throw RuntimeException("文件大小校验失败")
        }

        opts.verifyHash?.let { want ->
            emit(mapOf("type" to "verify", "stage" to "hash", "percent" to 0.0))
            val got = hashFile(output, opts.verifyAlgo) { pct ->
                emit(mapOf("type" to "verify", "stage" to "hash", "percent" to pct))
            }
            if (!got.equals(want, ignoreCase = true)) {
                emit(mapOf("type" to "error", "message" to "Hash 校验不匹配"))
                throw RuntimeException("Hash 校验失败")
            }
        }

        val elapsed = (System.currentTimeMillis() - t0) / 1000.0
        emit(
            mapOf(
                "type" to "done",
                "size" to probe.size,
                "elapsed" to elapsed,
                "avg_speed" to (if (elapsed > 0) (probe.size / elapsed).toLong() else 0L),
                "peak_speed" to peak.get(),
                "path" to output.absolutePath
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
        while (attempt < 4) {
            if (cancelled) throw CancelledException()
            val req = Request.Builder().url(url)
                .header("Range", "bytes=${c.start}-${c.endIncl}")
                .header("Accept-Encoding", "identity")
                .apply { headers.forEach { (k, v) -> header(k, v) } }
                .build()
            try {
                client.newCall(req).execute().use { resp ->
                    if (resp.code == 429 || resp.code == 503) {
                        throw RateLimitedException((resp.header("Retry-After")?.toLongOrNull() ?: 2L) * 1000L)
                    }
                    if (resp.code == 416) {
                        // 已下载完整
                        doneBytes.addAndGet(c.length)
                        return@withContext
                    }
                    if (resp.code !in 200..299) throw RuntimeException("HTTP ${resp.code}")
                    val body = resp.body ?: throw RuntimeException("空响应体")
                    val input = body.byteStream()
                    raf.seek(c.start)
                    val buf = ByteArray(64 * 1024)
                    var pos = c.start
                    while (pos <= c.endIncl) {
                        if (cancelled) throw CancelledException()
                        val want = min(buf.size.toLong(), c.endIncl - pos + 1).toInt()
                        val n = input.read(buf, 0, want)
                        if (n < 0) break
                        raf.write(buf, 0, n)
                        pos += n
                        doneBytes.addAndGet(n.toLong())
                        bucket.take(n.toLong())
                    }
                    if (pos <= c.endIncl) throw RuntimeException("连接提前中断")
                    return@withContext
                }
            } catch (e: RateLimitedException) {
                attempt++
                delay(e.waitMs.coerceIn(1000L, 8000L))
            } catch (e: CancelledException) {
                throw e
            } catch (e: Exception) {
                attempt++
                if (attempt >= 4) throw e
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
