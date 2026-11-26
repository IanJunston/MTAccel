package com.ianjunston.mtaccel

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.Build
import android.os.IBinder
import androidx.core.app.NotificationCompat

/**
 * 前台服务：保证后台下载不被系统回收，并在通知栏展示进度。
 */
class DownloadService : Service() {

    companion object {
        private const val CHANNEL_ID = "mtaccel_download"
        private const val NOTIF_ID = 8801

        @Volatile
        private var instance: DownloadService? = null

        fun onEngineEvent(ev: Map<String, Any?>) {
            instance?.handle(ev)
        }
    }

    private var lastNotify = 0L

    override fun onCreate() {
        super.onCreate()
        instance = this
        createChannel()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        startForeground(NOTIF_ID, buildNotification("MTAccel 已就绪", "等待下载任务", -1, false))
        return START_STICKY
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onDestroy() {
        instance = null
        super.onDestroy()
    }

    private fun createChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val ch = NotificationChannel(
                CHANNEL_ID,
                getString(R.string.channel_name),
                NotificationManager.IMPORTANCE_LOW
            )
            ch.description = getString(R.string.channel_desc)
            ch.setShowBadge(false)
            val nm = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
            nm.createNotificationChannel(ch)
        }
    }

    private fun buildNotification(
        title: String,
        text: String,
        progress: Int,
        ongoing: Boolean
    ): Notification {
        val pi = PendingIntent.getActivity(
            this, 0,
            Intent(this, MainActivity::class.java).apply {
                flags = Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_CLEAR_TOP
            },
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

        val builder = NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle(title)
            .setContentText(text)
            .setSmallIcon(android.R.drawable.stat_sys_download)
            .setContentIntent(pi)
            .setOngoing(ongoing)
            .setOnlyAlertOnce(true)
            .setPriority(NotificationCompat.PRIORITY_LOW)

        if (progress in 0..100) {
            builder.setProgress(100, progress, false)
        } else if (progress < 0 && ongoing) {
            builder.setProgress(0, 0, true)
        }
        return builder.build()
    }

    private fun notify(title: String, text: String, progress: Int, ongoing: Boolean) {
        val nm = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        nm.notify(NOTIF_ID, buildNotification(title, text, progress, ongoing))
    }

    private fun handle(ev: Map<String, Any?>) {
        when (ev["type"]) {
            "progress" -> {
                val now = System.currentTimeMillis()
                if (now - lastNotify < 700) return
                lastNotify = now
                val pct = ((ev["percent"] as? Double) ?: 0.0).toInt().coerceIn(0, 100)
                val speed = (ev["speed"] as? Long) ?: 0L
                val done = (ev["confirmed"] as? Long) ?: 0L
                val total = (ev["total"] as? Long) ?: 0L
                notify(
                    "下载中 $pct%",
                    "${fmt(done)} / ${fmt(total)} · ${fmt(speed)}/s",
                    pct,
                    true
                )
            }

            "done" -> notify(
                "下载完成",
                (ev["path"] as? String) ?: "已完成",
                -2,
                false
            )

            "error" -> notify(
                "下载失败",
                (ev["message"] as? String) ?: "未知错误",
                -2,
                false
            )

            "state" -> {
                val st = ev["state"] as? String
                if (st == "probing") notify("正在探测…", "分析服务器能力", -1, true)
            }
        }
    }

    private fun fmt(v: Long): String {
        if (v <= 0) return "0 B"
        val units = arrayOf("B", "KB", "MB", "GB", "TB")
        var x = v.toDouble()
        var i = 0
        while (x >= 1024 && i < units.size - 1) {
            x /= 1024
            i++
        }
        return String.format("%.1f %s", x, units[i])
    }
}
