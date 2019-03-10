package com.riywo.ninja.bptree

import org.apache.avro.Schema

class Table(keySchema: Schema, valueSchema: Schema) {
    val key = AvroRecordIO(keySchema)
    val value = AvroRecordIO(valueSchema)

    init {
        val isIgnore = { f: Schema.Field -> f.order() == Schema.Field.Order.IGNORE }
        if (keySchema.fields.any(isIgnore)) {
            throw IllegalArgumentException("All fields of keySchema must be ordered: $keySchema")
        }
        if (valueSchema.fields.none(isIgnore)) {
            throw IllegalArgumentException("All fields of valueSchema must be ignored: $valueSchema")
        }
    }


}
