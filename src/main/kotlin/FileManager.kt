package com.riywo.ninja.bptree

import FileMetadata
import NodeType
import KeyValue
import java.io.EOFException
import java.io.RandomAccessFile
import java.lang.Exception

class FileManager private constructor(filePath: String, initialMetadata: FileMetadata? = null) {
    companion object {
        val metadataOffset = emptyFileMetadata.toByteBuffer().limit()

        fun new(filePath: String): FileManager {
            val fileManager = FileManager(filePath, createFileMetadata())
            val rootPage = fileManager.allocate(NodeType.LeafNode, mutableListOf())
            fileManager.write(rootPage)
            return fileManager
        }

        fun load(filePath: String) = FileManager(filePath)
    }

    private val file = RandomAccessFile(filePath, "rws")
    private val metadata = initialMetadata ?: loadMetadata()
    private val buffer = ByteArray(MAX_PAGE_SIZE)
    val fileSize get() = file.length()
    private var nextFreePageId: Int? by metadata

    fun allocate(nodeType: NodeType, initialRecords: MutableList<KeyValue>): Page {
        val freePageId = nextFreePageId ?: throw Exception() // TODO
        val freePage = read(freePageId)
        nextFreePageId = if (freePage == null) {
            freePageId + 1
        } else {
            freePage.nextId ?: throw Exception() // TODO
        }
        writeMetadata()
        return Page.new(freePageId, nodeType, initialRecords)
    }

    fun read(id: Int): Page? {
        return try {
            seek(id)
            file.readFully(buffer)
            Page.load(buffer.toByteBuffer())
        } catch (e: EOFException) {
            null
        }
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