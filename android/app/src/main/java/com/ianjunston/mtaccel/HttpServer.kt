package com.ianjunston.mtaccel

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.URLDecoder
import java.nio.charset.StandardCharsets

data class HttpReq(
    val method: String,
    val path: String,
    val query: Map<String, String>,
    val headers: Map<String, String>,
    val body: ByteArray
) {
    fun bodyString(): String = String(body, StandardCharsets.UTF_8)
}

class HttpResp(
    val status: Int,
    val contentType: String,
    val body: ByteArray
) {
    /** 非空时走流式输出（SSE），body 忽略 */
    var streamer: ((OutputStream) -> Unit)? = null

    companion object {
        fun text(s: String, status: Int = 200): HttpResp =
            HttpResp(status, "text/plain; charset=utf-8", s.toByteArray(StandardCharsets.UTF_8))

        fun json(s: String, status: Int = 200): HttpResp =
            HttpResp(status, "application/json; charset=utf-8", s.toByteArray(StandardCharsets.UTF_8))

        fun html(bytes: ByteArray): HttpResp =
            HttpResp(200, "text/html; charset=utf-8", bytes)

        fun notFound(): HttpResp = text("404 Not Found", 404)

        fun sse(writer: (OutputStream) -> Unit): HttpResp {
            val r = HttpResp(200, "text/event-stream; charset=utf-8", ByteArray(0))
            r.streamer = writer
            return r
        }
    }
}

class HttpServer(
    private val port: Int,
    private val handler: (HttpReq) -> HttpResp
) {
    private var server: ServerSocket? = null
    @Volatile
    private var running = false

    fun start(): Int {
        val ss = ServerSocket(port, 64, InetAddress.getByName("127.0.0.1"))
        server = ss
        running = true
        Thread({
            while (running) {
                val s = try {
                    ss.accept()
                } catch (e: Exception) {
                    break
                }
                Thread({ serve(s) }, "http-conn").apply { isDaemon = true }.start()
            }
        }, "http-accept").apply { isDaemon = true }.start()
        return ss.localPort
    }

    fun stop() {
        running = false
        runCatching { server?.close() }
    }

    private fun serve(socket: Socket) {
        try {
            socket.use { s ->
                s.soTimeout = 0
                val input = BufferedInputStream(s.getInputStream())
                val output = BufferedOutputStream(s.getOutputStream())

                val requestLine = readLine(input) ?: return
                val parts = requestLine.split(" ")
                if (parts.size < 2) return
                val method = parts[0].uppercase()
                val rawTarget = parts[1]

                val headers = HashMap<String, String>()
                while (true) {
                    val line = readLine(input) ?: break
                    if (line.isEmpty()) break
                    val idx = line.indexOf(':')
                    if (idx > 0) {
                        headers[line.substring(0, idx).trim().lowercase()] =
                            line.substring(idx + 1).trim()
                    }
                }

                val contentLength = headers["content-length"]?.toIntOrNull() ?: 0
                val body = if (contentLength > 0) {
                    val b = ByteArray(contentLength)
                    var off = 0
                    while (off < contentLength) {
                        val n = input.read(b, off, contentLength - off)
                        if (n < 0) break
                        off += n
                    }
                    b
                } else ByteArray(0)

                val qIdx = rawTarget.indexOf('?')
                val path = if (qIdx >= 0) rawTarget.substring(0, qIdx) else rawTarget
                val query = HashMap<String, String>()
                if (qIdx >= 0) {
                    rawTarget.substring(qIdx + 1).split('&').forEach { kv ->
                        if (kv.isNotEmpty()) {
                            val e = kv.indexOf('=')
                            if (e > 0) {
                                query[URLDecoder.decode(kv.substring(0, e), "UTF-8")] =
                                    URLDecoder.decode(kv.substring(e + 1), "UTF-8")
                            } else {
                                query[URLDecoder.decode(kv, "UTF-8")] = ""
                            }
                        }
                    }
                }

                val req = HttpReq(method, path, query, headers, body)
                val resp = try {
                    handler(req)
                } catch (e: Exception) {
                    HttpResp.json("{\"error\":\"${jsonEscape(e.message ?: e.javaClass.simpleName)}\"}", 500)
                }

                val head = StringBuilder()
                head.append("HTTP/1.1 ${resp.status} ${statusText(resp.status)}\r\n")
                head.append("Content-Type: ${resp.contentType}\r\n")
                head.append("Cache-Control: no-store\r\n")
                head.append("Access-Control-Allow-Origin: *\r\n")
                if (resp.streamer != null) {
                    head.append("Connection: keep-alive\r\n")
                    head.append("X-Accel-Buffering: no\r\n")
                    head.append("\r\n")
                    output.write(head.toString().toByteArray(StandardCharsets.UTF_8))
                    output.flush()
                    resp.streamer?.invoke(output)
                } else {
                    head.append("Content-Length: ${resp.body.size}\r\n")
                    head.append("Connection: close\r\n\r\n")
                    output.write(head.toString().toByteArray(StandardCharsets.UTF_8))
                    output.write(resp.body)
                    output.flush()
                }
            }
        } catch (_: Exception) {
            // 连接异常直接丢弃
        }
    }

    private fun statusText(code: Int): String = when (code) {
        200 -> "OK"
        400 -> "Bad Request"
        404 -> "Not Found"
        409 -> "Conflict"
        500 -> "Internal Server Error"
        else -> "OK"
    }

    private fun readLine(input: InputStream): String? {
        val sb = StringBuilder()
        while (true) {
            val b = input.read()
            if (b < 0) return if (sb.isEmpty()) null else sb.toString()
            if (b == '\n'.code) break
            if (b != '\r'.code) sb.append(b.toChar())
        }
        return sb.toString()
    }
}

// ---------------- JSON 极简序列化 ----------------

fun jsonEscape(s: String): String {
    val sb = StringBuilder()
    for (c in s) {
        when (c) {
            '"' -> sb.append("\\\"")
            '\\' -> sb.append("\\\\")
            '\n' -> sb.append("\\n")
            '\r' -> sb.append("\\r")
            '\t' -> sb.append("\\t")
            else -> if (c.code < 0x20) sb.append("\\u%04x".format(c.code)) else sb.append(c)
        }
    }
    return sb.toString()
}

fun toJson(v: Any?): String = when (v) {
    null -> "null"
    is String -> "\"${jsonEscape(v)}\""
    is Boolean -> v.toString()
    is Int, is Long, is Short, is Byte -> v.toString()
    is Double -> if (v.isNaN() || v.isInfinite()) "0" else String.format("%.3f", v)
    is Float -> if (v.isNaN() || v.isInfinite()) "0" else String.format("%.3f", v.toDouble())
    is Map<*, *> -> v.entries.joinToString(",", "{", "}") { (k, value) ->
        "\"${jsonEscape(k.toString())}\":${toJson(value)}"
    }
    is List<*> -> v.joinToString(",", "[", "]") { toJson(it) }
    else -> "\"${jsonEscape(v.toString())}\""
}
