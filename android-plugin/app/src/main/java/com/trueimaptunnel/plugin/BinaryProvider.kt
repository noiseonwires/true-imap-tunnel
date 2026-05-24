package com.trueimaptunnel.plugin

import android.net.Uri
import android.os.ParcelFileDescriptor
import com.github.shadowsocks.plugin.NativePluginProvider
import com.github.shadowsocks.plugin.PathProvider
import java.io.File
import java.io.FileNotFoundException

class BinaryProvider : NativePluginProvider() {
    override fun populateFiles(provider: PathProvider) {
        provider.addPath("true-imap-tunnel", 0b111101101)
    }

    override fun getExecutable(): String =
        context!!.applicationInfo.nativeLibraryDir + "/libtrueimaptunnel.so"

    override fun openFile(uri: Uri): ParcelFileDescriptor = when (uri.path) {
        "/true-imap-tunnel", "/tits" ->
            ParcelFileDescriptor.open(File(getExecutable()), ParcelFileDescriptor.MODE_READ_ONLY)
        else -> throw FileNotFoundException()
    }
}
