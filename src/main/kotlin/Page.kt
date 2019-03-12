package com.riywo.ninja.bptree

import PageData
import java.nio.ByteBuffer

interface Page {
    fun id(): Int
    fun size(): Int
    fun records(): List<ByteBuffer>
    fun dump(): ByteBuffer
    fun get(keyByteBuffer: ByteBuffer): ByteBuffer?
    fun put(keyByteBuffer: ByteBuffer, recordByteBuffer: ByteBuffer)
    fun delete(keyByteBuffer: ByteBuffer)
}

class AvroPage private constructor(
    private val key: AvroGenericRecord.IO,
    private val data: PageData,
    private var byteSize: Int
) : Page {
    companion object {
        fun new(table: Table, id: Int): Page {
            val data = PageData(id, mutableListOf<ByteBuffer>())
            return load(table, data.toByteBuffer())
        }

        fun load(table: Table, byteBuffer: ByteBuffer): Page {
            val key = table.key
            val data = PageData.fromByteBuffer(byteBuffer)
            val size = byteBuffer.limit()
            return AvroPage(key, data, size)
        }
    }

    override fun id(): Int = data.getId()

    override fun size(): Int = byteSize

    override fun records(): List<ByteBuffer> = data.getRecords()

    override fun dump(): ByteBuffer = data.toByteBuffer()

    override fun get(keyByteBuffer: ByteBuffer): ByteBuffer? {
        val result = findKey(keyByteBuffer)
        return when(result) {
            is FindKeyResult.Found -> result.byteBuffer
            is FindKeyResult.NotFound -> null
        }
    }

    override fun put(keyByteBuffer: ByteBuffer, recordByteBuffer: ByteBuffer) {
        val result = findKey(keyByteBuffer)
        when(result) {
            is FindKeyResult.Found -> update(result.index, recordByteBuffer, result.byteBuffer)
            is FindKeyResult.NotFound -> insert(result.lastIndex, recordByteBuffer)
        }
    }

    override fun delete(keyByteBuffer: ByteBuffer) {
        val result = findKey(keyByteBuffer)
        if (result is FindKeyResult.Found) {
            data.getRecords().removeAt(result.index)
            byteSize -= result.byteBuffer.limit() + 1
            if (records().isEmpty()) byteSize -= 1
        }
    }

    private sealed class FindKeyResult {
        data class Found(val index: Int, val byteBuffer: ByteBuffer) : FindKeyResult()
        data class NotFound(val lastIndex: Int): FindKeyResult()
    }

    private fun findKey(keyByteBuffer: ByteBuffer): FindKeyResult {
        val keyBytes = keyByteBuffer.toByteArray(AVRO_RECORD_HEADER_SIZE)
        val records = records()
        records.forEachIndexed { index, byteBuffer ->
            val bytes = byteBuffer.toByteArray(AVRO_RECORD_HEADER_SIZE)
            when(key.compare(bytes, keyBytes)) {
                0 -> return FindKeyResult.Found(index, byteBuffer)
                1 -> return FindKeyResult.NotFound(index - 1)
            }
        }
        return FindKeyResult.NotFound(records.size)
    }

    private fun insert(index: Int, byteBuffer: ByteBuffer) {
        var newByteSize = byteSize + byteBuffer.limit() + 1
        if (records().isEmpty()) newByteSize += 1
        if (newByteSize > MAX_PAGE_SIZE) {
            throw PageFullException("Can't insert record")
        } else {
            data.getRecords().add(index, byteBuffer)
            byteSize = newByteSize
        }
    }

    private fun update(index: Int, newByteBuffer: ByteBuffer, oldByteBuffer: ByteBuffer) {
        val newByteSize = byteSize + newByteBuffer.limit() - oldByteBuffer.limit()
        if (newByteSize > MAX_PAGE_SIZE) {
            throw PageFullException("Can't update record")
        } else {
            // TODO merge new and old
            data.getRecords()[index] = newByteBuffer
            byteSize = newByteSize
        }
    }
}