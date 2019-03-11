package com.riywo.ninja.bptree

import org.apache.avro.Schema
import org.apache.avro.generic.*
import java.io.ByteArrayInputStream

class Table(schema: Schema) {
    val record: AvroRecordIO
    val key: AvroRecordIO
    val value: AvroRecordIO

    init {
        val isOrdered = { f: Schema.Field -> f.order() != Schema.Field.Order.IGNORE }
        val newField = { f: Schema.Field -> Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order()) }
        val keyFields = schema.fields.takeWhile(isOrdered)
        val valueFields = schema.fields.dropWhile(isOrdered)
        if (keyFields.isEmpty()) {
            throw IllegalArgumentException("At least the first field must be ordered: $schema")
        }
        if (valueFields.any(isOrdered)) {
            throw IllegalArgumentException("No ordered field is allowed after the first ignored field: $schema")
        }
        val keySchema = Schema.createRecord(keyFields.map(newField))
        val valueSchema = Schema.createRecord(valueFields.map(newField))
        record = AvroRecordIO(schema)
        key = AvroRecordIO(keySchema)
        value = AvroRecordIO(valueSchema)
    }

    open class AvroRecord(private val io: AvroRecordIO) : GenericData.Record(io.schema) {
        fun load(input: ByteArrayInputStream) {
            io.read(this, input)
        }

        fun load(bytes: ByteArray) {
            io.read(this, bytes)
        }
    }

    inner class Record : AvroRecord(record) {
        fun getKeyBytes(): ByteArray {
            return table.key.write(this)
        }
    }
    inner class Key : AvroRecord(key)
    inner class Value : AvroRecord(value)
}
