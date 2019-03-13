package com.riywo.ninja.bptree

import PageData
import org.apache.avro.generic.GenericRecord
import java.nio.ByteBuffer
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty

class AvroPage private constructor(
    private val keyIO: AvroGenericRecord.IO,
    private val recordIO: AvroGenericRecord.IO,
    private val data: PageData,
    private var byteSize: Int
) : Page {
    companion object {
        fun new(key: AvroGenericRecord.IO, record: AvroGenericRecord.IO, id: Int): Page {
            val builder = PageData.newBuilder()
            builder.id = id
            builder.records = mutableListOf<ByteBuffer>()
            builder.sentinelId = AVRO_ID_NULL_VALUE
            builder.previousId = AVRO_ID_NULL_VALUE
            builder.nextId = AVRO_ID_NULL_VALUE
            val data = builder.build()
            return load(key, record, data.toByteBuffer())
        }

        fun load(key: AvroGenericRecord.IO, record: AvroGenericRecord.IO, byteBuffer: ByteBuffer): Page {
            val data = PageData.fromByteBuffer(byteBuffer)
            val size = byteBuffer.limit()
            return AvroPage(key, record, data, size)
        }
    }

    override val id: Int get() = data.getId()
    override val size: Int get() = byteSize
    override val recordsSize: Int get() = data.getRecords().size
    override var sentinelId: Int? by NullableIntDelegate()
    override var previousId: Int? by NullableIntDelegate()
    override var nextId: Int? by NullableIntDelegate()

    override fun records(): List<GenericRecord> = data.getRecords().map{
        val record = AvroGenericRecord(recordIO)
        recordIO.decode(record, it)
        record
    }

    private class NullableIntDelegate : ReadWriteProperty<AvroPage, Int?> {
        override fun getValue(thisRef: AvroPage, property: KProperty<*>): Int? {
            println(property.name)
            val value = thisRef.data.get(property.name) as Int
            return if (value == AVRO_ID_NULL_VALUE) null else value
        }

        override fun setValue(thisRef: AvroPage, property: KProperty<*>, value: Int?) {
            thisRef.data.put(property.name, value ?: AVRO_ID_NULL_VALUE)
        }
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
            data.getRecords().removeAt(result.index)
            byteSize -= result.byteBuffer.limit() + 1 // 1 == Bytes length
            if (recordsSize == 0) byteSize -= 1 // 1 == Array length
        }
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
        var newByteSize = byteSize + byteBuffer.limit() + 1 // 1 == Bytes length
        if (recordsSize == 0) newByteSize += 1 // 1 == Array length
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
