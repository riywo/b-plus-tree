package com.riywo.ninja.bptree

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import java.nio.ByteBuffer

class Table(schema: Schema) {
    val key: AvroGenericRecord.IO
    val record: AvroGenericRecord.IO
    val internal: AvroGenericRecord.IO

    init {
        val isOrdered = { f: Schema.Field -> f.order() != Schema.Field.Order.IGNORE }
        val newField = { f: Schema.Field -> Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order()) }
        val keyFields = schema.fields.takeWhile(isOrdered)
        val valueFields = schema.fields.dropWhile(isOrdered)
        if (keyFields.isEmpty()) {
            throw IllegalArgumentException("At least the first field must be ordered: $schema")
        }
        if (valueFields.isEmpty()) {
            throw IllegalArgumentException("At least one order ignored field is required: $schema")
        }
        if (valueFields.any(isOrdered)) {
            throw IllegalArgumentException("No ordered field is allowed after the first ignored field: $schema")
        }
        record = AvroGenericRecord.IO(schema)

        val keySchema = Schema.createRecord(keyFields.map(newField))
        key = AvroGenericRecord.IO(keySchema)

        val idField = Schema.Field(INTERNAL_ID_FIELD_NAME,
            SchemaBuilder.builder().intType(), "", 0, Schema.Field.Order.IGNORE)
        val internalSchema = Schema.createRecord((keyFields + idField).map(newField))
        internal = AvroGenericRecord.IO(internalSchema)
    }

    fun createRecord(byteBuffer: ByteBuffer): Record {
        val record = Record()
        record.load(byteBuffer)
        return record
    }

    fun createKey(byteBuffer: ByteBuffer): Key {
        val record = Key()
        record.load(byteBuffer)
        return record
    }

    inner class Record : AvroGenericRecord(record)
    inner class Key : AvroGenericRecord(key)
}
