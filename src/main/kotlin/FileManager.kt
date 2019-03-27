package com.riywo.ninja.bptree

import FileMetadata
import java.io.RandomAccessFile

class FileManager private constructor(filePath: String, initial: Boolean = false) {
    companion object {
        val metadataOffset = emptyFileMetadata.toByteBuffer().limit()
        fun new(filePath: String) = FileManager(filePath, true)
        fun load(filePath: String) = FileManager(filePath)
    }

    private val metadata: FileMetadata
    private val file = RandomAccessFile(filePath, "rws")
    private val buffer = ByteArray(MAX_PAGE_SIZE)
    val fileSize get() = file.length()
    private var lastFreedPageId
        get() = metadata.getLastFreedPageId()
        set(value) { metadata.setLastFreedPageId(value) }

    init {
        if (initial) {
            metadata = createFileMetadata()
            writeMetadata()
            val rootPage = Page.new(ROOT_PAGE_ID, NodeType.LeafNode, mutableListOf())
            write(rootPage)
        } else {
            metadata = loadMetadata()
        }
    }

    fun read(id: Int): Page {
        seek(id)
        file.readFully(buffer)
        return Page.load(buffer.toByteBuffer())
    }

    fun write(page: Page) {
        seek(page.id)
        page.dump().toByteArray(buffer)
        file.write(buffer)
    }

    private fun loadMetadata(): FileMetadata {
        val metadataBuffer = ByteArray(metadataOffset)
        file.seek(0)
        file.readFully(metadataBuffer)
        return FileMetadata.fromByteBuffer(metadataBuffer.toByteBuffer())
    }

    private fun writeMetadata() {
        file.seek(0)
        file.write(metadata.toByteBuffer().toByteArray())
    }

    private fun seek(id: Int) {
        val pos = id * MAX_PAGE_SIZE + metadataOffset
        file.seek(pos.toLong())
    }
}