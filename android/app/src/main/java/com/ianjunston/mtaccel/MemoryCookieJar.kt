package com.ianjunston.mtaccel

import okhttp3.Cookie
import okhttp3.CookieJar
import okhttp3.HttpUrl

/**
 * 内存 Cookie 仓库。
 *
 * 飞书这类网盘的分享下载链接在没带登录态时会 302 到 passport 登录页，
 * 服务端在那里下发一套匿名访客 cookie（passport_trace_id / swp_csrf_token /
 * session 等）。跟随重定向回到原链接时必须把这些 cookie 带上，否则服务端
 * 直接返回 401 Login Required。
 *
 * OkHttp 默认用 CookieJar.NO_COOKIES，重定向回来两手空空，所以必须自己存。
 */
class MemoryCookieJar : CookieJar {

    private val store = ArrayList<Cookie>()

    override fun saveFromResponse(url: HttpUrl, cookies: List<Cookie>) {
        if (cookies.isEmpty()) return
        synchronized(store) {
            for (c in cookies) {
                store.removeAll { it.name == c.name && it.domain == c.domain && it.path == c.path }
                store.add(c)
            }
        }
    }

    override fun loadForRequest(url: HttpUrl): List<Cookie> {
        val now = System.currentTimeMillis()
        synchronized(store) {
            store.removeAll { it.expiresAt < now }
            return store.filter { it.matches(url) }
        }
    }

    /** 换链接 / 换账号时清空，避免串味。 */
    fun clear() {
        synchronized(store) { store.clear() }
    }
}
