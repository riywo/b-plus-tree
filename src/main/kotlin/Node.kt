package com.riywo.ninja.bptree

import org.apache.avro.generic.GenericRecord
import java.nio.ByteBuffer

abstract class Node(
    private val keyIO: AvroGenericRecord.IO,
    private val recordIO: AvroGenericRecord.IO,
    protected val page: AvroPage
) {
    val id get() = page.id
    val previousId get() = page.previousId
    val nextId get() = page.nextId
    val size get() = page.size

    fun dump() = page.dump()

    fun getRecords(): List<GenericRecord> = page.records.map {
        val record = AvroGenericRecord(recordIO)
        recordIO.decode(record, it)
        record
    }

    fun get(key: GenericRecord): GenericRecord? {
        val result = find(key)
        return when (result) {
            is FindResult.ExactMatch -> createRecord(result.byteBuffer)
            else -> null
        }
    }

    fun put(record: GenericRecord) {
        val recordByteBuffer = recordIO.encode(record)
        val result = find(record)
        when(result) { // TODO merge new and old record
            is FindResult.ExactMatch -> page.update(result.index, recordByteBuffer, result.byteBuffer)
            is FindResult.LeftMatch -> page.insert(result.index, recordByteBuffer)
            is FindResult.RightMatch -> page.insert(result.index, recordByteBuffer)
        }
    }

    fun delete(key: GenericRecord) {
        val result = find(key)
        if (result is FindResult.ExactMatch) page.delete(result.index, result.byteBuffer)
    }

    protected sealed class FindResult {
        data class ExactMatch(val index: Int, val byteBuffer: ByteBuffer) : FindResult()
        data class LeftMatch(val index: Int, val byteBuffer: ByteBuffer) : FindResult()
        data class RightMatch(val index: Int) : FindResult()
    }

    protected fun find(key: GenericRecord): FindResult {
        val keyByteBuffer = keyIO.encode(key)
        val keyBytes = keyByteBuffer.toByteArray(AVRO_RECORD_HEADER_SIZE)
        page.records.forEachIndexed { index, byteBuffer ->
            val bytes = byteBuffer.toByteArray(AVRO_RECORD_HEADER_SIZE)
            when(keyIO.compare(bytes, keyBytes)) {
                0 -> return FindResult.ExactMatch(index, byteBuffer)
                1 -> return FindResult.LeftMatch(index, byteBuffer)
            }
        }
        return FindResult.RightMatch(page.records.size)
    }

    protected fun createRecord(byteBuffer: ByteBuffer): AvroGenericRecord {
        val record = AvroGenericRecord(recordIO)
        record.load(byteBuffer)
        return record
    }
}