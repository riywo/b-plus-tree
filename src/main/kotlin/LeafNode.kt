package com.riywo.ninja.bptree

import LeafNodePage
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.*
import org.apache.avro.specific.*
import java.io.*
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

    val records: List<Table.Record>
        get() = page.getRecords().map {
            val record = table.Record()
            record.load(it.toByteArray())
            record
        }

    fun get(keyBytes: ByteArray): ByteArray? {
        val result = findKey(keyBytes)
        return when(result) {
            is FindKeyResult.Found -> result.bytes
            is FindKeyResult.NotFound -> null
        }
    }

    fun get(key: Table.Key): Table.Record? {
        val keyBytes = table.key.encode(key)
        val bytes = get(keyBytes)
        return if (bytes == null) {
            null
        } else {
            val record = table.Record()
            record.load(bytes)
            record
        }
    }

    fun put(keyBytes: ByteArray, valueBytes: ByteArray) {
        val recordBytes = keyBytes + valueBytes
        val result = findKey(keyBytes)
        when(result) {
            is FindKeyResult.Found -> update(result.putIndex, recordBytes)
            is FindKeyResult.NotFound -> insert(result.putIndex, recordBytes)
        }
    }

    fun put(key: GenericRecord, value: GenericRecord) {
        val keyBytes = table.key.encode(key)
        val valueBytes = table.value.encode(value)
        put(keyBytes, valueBytes)
    }

    private sealed class FindKeyResult {
        data class Found(val putIndex: Int, val bytes: ByteArray) : FindKeyResult()
        data class NotFound(val putIndex: Int): FindKeyResult()
    }

    private fun findKey(keyBytes: ByteArray): FindKeyResult {
        val records = page.getRecords()
        records.forEachIndexed { index, byteBuffer ->
            val bytes = byteBuffer.toByteArray()
            when(table.key.compare(bytes, keyBytes)) {
                0 -> return FindKeyResult.Found(index, bytes)
                1 -> return FindKeyResult.NotFound(index - 1)
            }
        }
        return FindKeyResult.NotFound(records.size)
    }

    private fun insert(index: Int, bytes: ByteArray) {
        page.getRecords().add(index, bytes.toByteBuffer())
    }

    private fun update(index: Int, bytes: ByteArray) {
        page.getRecords()[index] = bytes.toByteBuffer()
    }
}
