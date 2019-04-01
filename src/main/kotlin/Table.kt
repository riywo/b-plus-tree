/**
 * Copyright 2019 Ryosuke IWANAGA <me@riywo.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.riywo.ninja.bptree

import org.apache.avro.Schema
import java.nio.ByteBuffer

class Table(keySchema: Schema, valueSchema: Schema) {
    val key: AvroGenericRecord.IO
    val value: AvroGenericRecord.IO
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
}
