package com.trueimaptunnel.plugin

import android.app.Activity
import android.graphics.Typeface
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.view.ViewGroup
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.atomic.AtomicBoolean

class MainActivity : Activity() {
    private val handler = Handler(Looper.getMainLooper())
    private val loading = AtomicBoolean(false)
    private lateinit var statusView: TextView
    private lateinit var logsView: TextView

    private val refreshTask = object : Runnable {
        override fun run() {
            refresh()
            handler.postDelayed(this, REFRESH_MS)
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        val root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(24, 24, 24, 24)
        }
        statusView = TextView(this).apply {
            textSize = 15f
            typeface = Typeface.MONOSPACE
            text = "Waiting for true-imap-tunnel..."
        }
        logsView = TextView(this).apply {
            textSize = 12f
            typeface = Typeface.MONOSPACE
        }
        root.addView(statusView, LinearLayout.LayoutParams(
            ViewGroup.LayoutParams.MATCH_PARENT,
            ViewGroup.LayoutParams.WRAP_CONTENT,
        ))
        root.addView(ScrollView(this).apply { addView(logsView) }, LinearLayout.LayoutParams(
            ViewGroup.LayoutParams.MATCH_PARENT,
            0,
            1f,
        ))
        setContentView(root)
    }

    override fun onResume() {
        super.onResume()
        handler.post(refreshTask)
    }

    override fun onPause() {
        handler.removeCallbacks(refreshTask)
        super.onPause()
    }

    private fun refresh() {
        if (!loading.compareAndSet(false, true)) return
        Thread {
            try {
                val statusRaw = get("/status")
                val logsRaw = get("/logs")
                val status = formatStatus(statusRaw)
                val logs = formatLogs(logsRaw)
                handler.post {
                    statusView.text = status
                    logsView.text = logs
                }
            } catch (e: Exception) {
                handler.post {
                    statusView.text = OFFLINE_TEXT
                    logsView.text = ""
                }
            } finally {
                loading.set(false)
            }
        }.start()
    }

    private fun get(path: String): String {
        val conn = (URL(BASE_URL + path).openConnection() as HttpURLConnection).apply {
            connectTimeout = 1000
            readTimeout = 1000
            requestMethod = "GET"
        }
        return try {
            conn.inputStream.bufferedReader().use { it.readText() }
        } finally {
            conn.disconnect()
        }
    }

    private fun formatStatus(raw: String): String {
        val root = JSONObject(raw)
        val tunnel = root.optJSONObject("tunnel") ?: return raw
        val sb = StringBuilder()
        sb.append("true-imap-tunnel online\n")
        sb.append("uptime: ").append(root.optLong("uptime_seconds")).append("s\n")
        sb.append("mode: ").append(tunnel.optString("mode")).append('\n')
        sb.append("streams: ").append(tunnel.optInt("active_streams")).append('\n')
        sb.append("accounts: ")
            .append(tunnel.optInt("accounts_connected")).append('/')
            .append(tunnel.optInt("accounts_total"))
            .append(" connected, ")
            .append(tunnel.optInt("accounts_proven"))
            .append(" proven\n")

        val accounts = tunnel.optJSONArray("accounts")
        if (accounts != null) {
            for (i in 0 until accounts.length()) {
                val a = accounts.optJSONObject(i) ?: continue
                sb.append('\n').append(a.optString("label", "account")).append('\n')
                sb.append("  send: ").append(if (a.optBoolean("sender_connected")) "up" else "down")
                    .append(", tx=").append(a.optLong("sent_frames"))
                    .append(", batches=").append(a.optLong("append_batches"))
                    .append('\n')
                sb.append("  recv: ").append(if (a.optBoolean("watcher_connected")) "up" else "down")
                    .append(", ready=").append(a.optBoolean("receive_ready"))
                    .append(", rx=").append(a.optLong("frames_received"))
                    .append('\n')
                if (a.has("idle_supported")) {
                    sb.append("  idle: ").append(a.optBoolean("idle_supported")).append('\n')
                }
                if (a.has("last_frame_age_ms")) {
                    sb.append("  last rx: ").append(a.optLong("last_frame_age_ms")).append("ms ago\n")
                }
            }
        }
        return sb.toString()
    }

    private fun formatLogs(raw: String): String {
        val entries = JSONObject(raw).optJSONArray("entries") ?: return raw
        val sb = StringBuilder("logs\n")
        for (i in 0 until entries.length()) {
            val e = entries.optJSONObject(i) ?: continue
            sb.append(e.optString("time").takeLast(18))
                .append(' ')
                .append(e.optString("level").uppercase())
                .append(' ')
                .append(e.optString("message"))
                .append('\n')
        }
        return sb.toString()
    }

    companion object {
        private const val BASE_URL = "http://127.123.45.67:17680"
        private const val REFRESH_MS = 3000L
        private const val OFFLINE_TEXT =
            "Tunnel status is not available.\n\n" +
                "This app is only a status monitor. Configure and start the " +
                "true-imap-tunnel plugin connection in Shadowsocks Android, " +
                "then return here to watch status and logs."
    }
}
