package com.riywo.ninja.bptree

import java.io.RandomAccessFile
import java.io.File

class FileManager(filePath: String) {
    private val file = RandomAccessFile(filePath, "rws")
    private val buffer = ByteArray(MAX_PAGE_SIZE)
    val fileSize get() = file.length()

    fun read(id: Int): Page {
        val pos = id * MAX_PAGE_SIZE
        file.seek(pos.toLong())
        file.readFully(buffer)
        return Page.load(buffer.toByteBuffer())
    }

    fun write(page: Page) {
        val pos = page.id * MAX_PAGE_SIZE
        file.seek(pos.toLong())
        page.dump().toByteArray(buffer)
        file.write(buffer)
    }
}