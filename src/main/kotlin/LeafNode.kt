package com.riywo.ninja.bptree

import LeafNodePage
import java.nio.ByteBuffer

const val AVRO_RECORD_HEADER_SIZE = 10

const val MAX_PAGE_SIZE = 4 * 1024

class LeafNodeFullException(message: String) : Exception(message)

class LeafNode(val table: Table, private val page: LeafNodePage) {
    companion object {
        fun new(table: Table): LeafNode {
            val page = LeafNodePage(mutableListOf<ByteBuffer>())
            return LeafNode(table, page)
        }

        fun load(table: Table, byteBuffer: ByteBuffer): LeafNode {
            val page = LeafNodePage.fromByteBuffer(byteBuffer)
            return LeafNode(table, page)
        }
    }

    fun dump(): ByteBuffer {
        return page.toByteBuffer()
    }

    fun pageSize(): Int {
        return dump().limit()
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
        page.getRecords().add(index, byteBuffer)
        if (isFull()) {
            page.getRecords().removeAt(index)
            throw LeafNodeFullException("Can't insert record")
        }
    }

    private fun update(index: Int, newByteBuffer: ByteBuffer, oldByteBuffer: ByteBuffer) {
        // TODO merge new and old
        page.getRecords()[index] = newByteBuffer
        if (isFull()) {
            page.getRecords()[index] = oldByteBuffer
            throw LeafNodeFullException("Can't update record")
        }
    }

    private fun isFull(): Boolean {
        return pageSize() > MAX_PAGE_SIZE // TODO approximate calculation
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
