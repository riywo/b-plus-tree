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

import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*

import org.apache.avro.SchemaBuilder

class TestAvroGenericRecord {
    @Nested
    class IO {
        private val schema = SchemaBuilder.record("test").fields()
            .name("f1").type().stringType().noDefault()
            .name("f2").type().intType().noDefault()
            .endRecord()

        private val io = AvroGenericRecord.IO(schema)

        @Test
        fun `encode and decode`() {
            val record1 = AvroGenericRecord(io)
            record1.put("f1", "aaa")
            record1.put("f2", 111)
            val encoded = io.encode(record1)
            val record2 = AvroGenericRecord(io)
            record2.load(encoded)
            assertThat(record2).isEqualTo(record1)

        }

        @Test
        fun `compare f1`() {
            val aRecord = AvroGenericRecord(io)
            aRecord.put("f1", "a")
            aRecord.put("f2", 111)
            val bRecord = AvroGenericRecord(io)
            bRecord.put("f1", "b")
            bRecord.put("f2", 111)

            val a = aRecord.toByteBuffer().toByteArray()
            val b = bRecord.toByteBuffer().toByteArray()

            assertThat(io.compare(a, a)).isEqualTo(0)
            assertThat(io.compare(a, b)).isEqualTo(-1)
            assertThat(io.compare(b, a)).isEqualTo(1)
        }

        @Test
        fun `compare f2`() {
            val aRecord = AvroGenericRecord(io)
            aRecord.put("f1", "a")
            aRecord.put("f2", 111)
            val bRecord = AvroGenericRecord(io)
            bRecord.put("f1", "a")
            bRecord.put("f2", 222)

            val a = aRecord.toByteBuffer().toByteArray()
            val b = bRecord.toByteBuffer().toByteArray()

            assertThat(io.compare(a, a)).isEqualTo(0)
            assertThat(io.compare(a, b)).isEqualTo(-1)
            assertThat(io.compare(b, a)).isEqualTo(1)
        }
    }
}