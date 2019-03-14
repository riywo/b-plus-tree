package com.riywo.ninja.bptree

import PageData
import org.apache.avro.generic.GenericRecord
import java.nio.ByteBuffer

class AvroPage(
    private val keyIO: AvroGenericRecord.IO,
    private val recordIO: AvroGenericRecord.IO,
    private val data: PageData,
    private var byteSize: Int = 0
) : Page {
    companion object {
        fun new(key: AvroGenericRecord.IO, record: AvroGenericRecord.IO, id: Int): Page {
            val data = createPageData(id)
            return AvroPage(key, record, data)
        }

        fun load(key: AvroGenericRecord.IO, record: AvroGenericRecord.IO, byteBuffer: ByteBuffer): Page {
            val data = PageData.fromByteBuffer(byteBuffer)
            return AvroPage(key, record, data, byteBuffer.limit())
        }
    }

    init {
        if (byteSize == 0) byteSize = dump().limit()
    }

    override val id: Int by data
    override var sentinelId: Int? by data
    override var previousId: Int? by data
    override var nextId: Int? by data
    override val records: List<ByteBuffer> get() = data.getRecords()
    override val size: Int get() = byteSize

    override fun records(): List<GenericRecord> = data.getRecords().map{
        val record = AvroGenericRecord(recordIO)
        recordIO.decode(record, it)
        record
    }

    override fun dump(): ByteBuffer = data.toByteBuffer()

    override fun get(key: GenericRecord): GenericRecord? {
        val keyByteBuffer = keyIO.encode(key)
        val result = findKey(keyByteBuffer)
        return when(result) {
            is FindKeyResult.Found -> {
                val found = AvroGenericRecord(recordIO)
                found.load(result.byteBuffer)
                found
            }
            is FindKeyResult.NotFound -> null
        }
    }

    override fun put(record: GenericRecord) {
        val keyByteBuffer = keyIO.encode(record)
        val recordByteBuffer = recordIO.encode(record)
        val result = findKey(keyByteBuffer)
        when(result) {
            is FindKeyResult.Found -> update(result.index, recordByteBuffer, result.byteBuffer)
            is FindKeyResult.NotFound -> insert(result.lastIndex, recordByteBuffer)
        }
    }

    override fun delete(key: GenericRecord) {
        val keyByteBuffer = keyIO.encode(key)
        val result = findKey(keyByteBuffer)
        if (result is FindKeyResult.Found) {
            byteSize = calcPageSize(-result.byteBuffer.toAvroBytesSize(), -1)
            data.getRecords().removeAt(result.index)
        }
    }

    private fun calcPageSize(changingBytes: Int, changingLength: Int = 0): Int {
        return byteSize + changingBytes + calcChangingLengthBytes(changingLength)
    }

    private fun calcChangingLengthBytes(changingLength: Int): Int {
        if (changingLength == 0) return 0
        val newLength = records.size + changingLength
        return newLength.toLengthAvroByteSize() - records.size.toLengthAvroByteSize()
    }

    private sealed class FindKeyResult {
        data class Found(val index: Int, val byteBuffer: ByteBuffer) : FindKeyResult()
        data class NotFound(val lastIndex: Int): FindKeyResult()
    }

    private fun findKey(keyByteBuffer: ByteBuffer): FindKeyResult {
        val keyBytes = keyByteBuffer.toByteArray(AVRO_RECORD_HEADER_SIZE)
        val records = records()
        data.getRecords().forEachIndexed { index, byteBuffer ->
            val bytes = byteBuffer.toByteArray(AVRO_RECORD_HEADER_SIZE)
            when(keyIO.compare(bytes, keyBytes)) {
                0 -> return FindKeyResult.Found(index, byteBuffer)
                1 -> return FindKeyResult.NotFound(index - 1)
            }
        }
        return FindKeyResult.NotFound(records.size)
    }

    private fun insert(index: Int, byteBuffer: ByteBuffer) {
        val newByteSize = calcPageSize(byteBuffer.toAvroBytesSize(), 1)
        if (newByteSize > MAX_PAGE_SIZE) {
            throw PageFullException("Can't insert record")
        } else {
            byteSize = newByteSize
            data.getRecords().add(index, byteBuffer)
        }
    }

    private fun update(index: Int, newByteBuffer: ByteBuffer, oldByteBuffer: ByteBuffer) {
        val newByteSize = calcPageSize(newByteBuffer.toAvroBytesSize() - oldByteBuffer.toAvroBytesSize())
        if (newByteSize > MAX_PAGE_SIZE) {
            throw PageFullException("Can't update record")
        } else {
            // TODO merge new and old
            byteSize = newByteSize
            data.getRecords()[index] = newByteBuffer
        }
    }
}
