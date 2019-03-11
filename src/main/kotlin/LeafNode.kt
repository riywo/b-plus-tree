package com.riywo.ninja.bptree

import Page
import java.nio.ByteBuffer

class LeafNode(val table: Table, private val page: Page, private var byteSize: Int) {
    companion object {
        fun new(table: Table): LeafNode {
            val page = Page(mutableListOf<ByteBuffer>())
            val byteSize = page.toByteBuffer().limit()
            return LeafNode(table, page, byteSize)
        }

        fun load(table: Table, byteBuffer: ByteBuffer): LeafNode {
            val page = Page.fromByteBuffer(byteBuffer)
            return LeafNode(table, page, byteBuffer.limit())
        }
    }

    fun dump(): ByteBuffer {
        return page.toByteBuffer()
    }

    fun pageSize(): Int {
        return byteSize
    }

    fun pageRecords(): List<ByteBuffer> {
        return page.getRecords()
    }

    fun get(keyByteBuffer: ByteBuffer): ByteBuffer? {
        val result = findKey(keyByteBuffer)
        return when(result) {
            is FindKeyResult.Found -> result.byteBuffer
            is FindKeyResult.NotFound -> null
        }
    }

    fun put(keyByteBuffer: ByteBuffer, recordByteBuffer: ByteBuffer) {
        val result = findKey(keyByteBuffer)
        when(result) {
            is FindKeyResult.Found -> update(result.index, recordByteBuffer, result.byteBuffer)
            is FindKeyResult.NotFound -> insert(result.lastIndex, recordByteBuffer)
        }
    }

    fun delete(keyByteBuffer: ByteBuffer) {
        val result = findKey(keyByteBuffer)
        if (result is FindKeyResult.Found) {
            page.getRecords().removeAt(result.index)
            byteSize -= result.byteBuffer.limit() + 1
            if (pageRecords().isEmpty()) byteSize -= 1
        }
    }

    private sealed class FindKeyResult {
        data class Found(val index: Int, val byteBuffer: ByteBuffer) : FindKeyResult()
        data class NotFound(val lastIndex: Int): FindKeyResult()
    }

    private fun findKey(keyByteBuffer: ByteBuffer): FindKeyResult {
        val keyBytes = keyByteBuffer.toByteArray(AVRO_RECORD_HEADER_SIZE)
        val records = pageRecords()
        records.forEachIndexed { index, byteBuffer ->
            val bytes = byteBuffer.toByteArray(AVRO_RECORD_HEADER_SIZE)
            when(table.key.compare(bytes, keyBytes)) {
                0 -> return FindKeyResult.Found(index, byteBuffer)
                1 -> return FindKeyResult.NotFound(index - 1)
            }
        }
        return FindKeyResult.NotFound(records.size)
    }

    private fun insert(index: Int, byteBuffer: ByteBuffer) {
        var newByteSize = byteSize + byteBuffer.limit() + 1
        if (pageRecords().isEmpty()) newByteSize += 1
        if (newByteSize > MAX_PAGE_SIZE) {
            throw PageFullException("Can't insert record")
        } else {
            page.getRecords().add(index, byteBuffer)
            byteSize = newByteSize
        }
    }

    private fun update(index: Int, newByteBuffer: ByteBuffer, oldByteBuffer: ByteBuffer) {
        val newByteSize = byteSize + newByteBuffer.limit() - oldByteBuffer.limit()
        if (newByteSize > MAX_PAGE_SIZE) {
            throw PageFullException("Can't update record")
        } else {
            // TODO merge new and old
            page.getRecords()[index] = newByteBuffer
            byteSize = newByteSize
        }
    }

    override fun hashCode(): Int {
        return page.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as LeafNode

        if (page != other.page) return false

        return true
    }
}
