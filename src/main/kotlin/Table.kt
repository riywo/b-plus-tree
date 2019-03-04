package com.riywo.ninja.bptree

import org.apache.avro.Schema
import java.lang.IllegalArgumentException

class Table(val schema: Schema) {
    val keySchema: Schema

    init {
        val keyFields = schema.fields.takeWhile { it.order() != Schema.Field.Order.IGNORE }
        val nonKeyFields = schema.fields.dropWhile { it.order() != Schema.Field.Order.IGNORE }
        if (keyFields.isEmpty()) {
            throw IllegalArgumentException("At least the first field must have order.")
        }
        if (nonKeyFields.any { it.order() != Schema.Field.Order.IGNORE }) {
            throw IllegalArgumentException("Non-key fields can't have order.")
        }
        val newField = { f: Schema.Field ->
            Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order()) }
        keySchema = Schema.createRecord(keyFields.map(newField))
    }

    inner class Record : AvroRecord(schema, AvroRecord.IO(schema))
    inner class Key : AvroRecord(keySchema, AvroRecord.IO(keySchema))
}
