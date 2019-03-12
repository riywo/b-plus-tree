package com.riywo.ninja.bptree

import org.apache.avro.Schema

class Table(schema: Schema) {
    val record: AvroGenericRecord.IO
    val key: AvroGenericRecord.IO

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

        record = AvroGenericRecord.IO(schema)
        val keySchema = Schema.createRecord(keyFields.map(newField))
        key = AvroGenericRecord.IO(keySchema)
    }

    inner class Record : AvroGenericRecord(record)
    inner class Key : AvroGenericRecord(key)
}
