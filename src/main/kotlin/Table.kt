package com.riywo.ninja.bptree

import org.apache.avro.Schema
import java.lang.Exception
import java.nio.ByteBuffer

class Table(keySchema: Schema, valueSchema: Schema) {
    val key: AvroGenericRecord.IO
    val value: AvroGenericRecord.IO
    val minimumKey: Key
    init {
        val isOrdered = { f: Schema.Field -> f.order() != Schema.Field.Order.IGNORE }
        if (!keySchema.fields.all(isOrdered)) {
            throw IllegalArgumentException("All key fields must be ordered: $keySchema")
        }
        val duplicatedFields = keySchema.fields.intersect(valueSchema.fields)
        if (duplicatedFields.isNotEmpty()) {
            throw IllegalArgumentException("Some fields are duplicated: $duplicatedFields")
        }
        key = AvroGenericRecord.IO(keySchema)
        value = AvroGenericRecord.IO(valueSchema)
        minimumKey = setMinimumKey(keySchema)
    }

    inner class Key : AvroGenericRecord(key)
    inner class Value : AvroGenericRecord(value)
    inner class Record(val key: Key, val value: Value)

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

    fun createRecord(keyByteBuffer: ByteBuffer, valueByteBuffer: ByteBuffer): Record {
        return Record(createKey(keyByteBuffer), createValue(valueByteBuffer))
    }

    fun createKey(json: String): Key {
        val record = Key()
        record.load(json)
        return record
    }

    fun createValue(json: String): Value {
        val record = Value()
        record.load(json)
        return record
    }

    fun createRecord(keyJson: String, valueJson: String): Record {
        return Record(createKey(keyJson), createValue(valueJson))
    }

    private fun setMinimumKey(schema: Schema): Key {
        val key = Key()
        schema.fields.forEach {
            when (it.schema().type) {
                Schema.Type.STRING -> key.put(it.name(), "")
                Schema.Type.INT -> key.put(it.name(), Int.MIN_VALUE)
                else -> throw Exception("Not supported field: $it")
            }
        }
        return key
    }
}
