package com.riywo.ninja.bptree

import org.apache.avro.Schema
import java.nio.ByteBuffer

class Table(keySchema: Schema, valueSchema: Schema) {
    val key: AvroGenericRecord.IO
    val value: AvroGenericRecord.IO

    init {
        val isOrdered = { f: Schema.Field -> f.order() != Schema.Field.Order.IGNORE }
        if (keySchema.fields.none(isOrdered)) {
            throw IllegalArgumentException("All key fields must be ordered: $keySchema")
        }
        if (valueSchema.fields.any(isOrdered)) {
            throw IllegalArgumentException("No ordered field is allowed in value: $valueSchema")
        }
        key = AvroGenericRecord.IO(keySchema)
        value = AvroGenericRecord.IO(valueSchema)
    }

    inner class Key : AvroGenericRecord(key)
    inner class Value : AvroGenericRecord(value)

    fun createKey(byteBuffer: ByteBuffer): Key {
        val record = Key()
        record.load(byteBuffer)
        return record
    }

    fun createValue(byteBuffer: ByteBuffer): Value {
        val record = Value()
        record.load(byteBuffer)
        return record
    }
}
