package com.riywo.ninja.bptree

import LeafNodePage
import java.nio.ByteBuffer

class LeafNode(private val table: Table, private val page: LeafNodePage) {
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
            is FindKeyResult.Found -> update(result.putIndex, recordByteBuffer)
            is FindKeyResult.NotFound -> insert(result.putIndex, recordByteBuffer)
        }
    }

    private sealed class FindKeyResult {
        data class Found(val putIndex: Int, val byteBuffer: ByteBuffer) : FindKeyResult()
        data class NotFound(val putIndex: Int): FindKeyResult()
    }

    private fun findKey(keyByteBuffer: ByteBuffer): FindKeyResult {
        val keyBytes = keyByteBuffer.toByteArray(10)
        val records = pageRecords()
        records.forEachIndexed { index, byteBuffer ->
            val bytes = byteBuffer.toByteArray(10)
            when(table.key.compare(bytes, keyBytes)) {
                0 -> return FindKeyResult.Found(index, byteBuffer)
                1 -> return FindKeyResult.NotFound(index - 1)
            }
        }
        return FindKeyResult.NotFound(records.size)
    }

    private fun insert(index: Int, byteBuffer: ByteBuffer) {
        page.getRecords().add(index, byteBuffer)
    }

    private fun update(index: Int, byteBuffer: ByteBuffer) {
        page.getRecords()[index] = byteBuffer
    }
}
