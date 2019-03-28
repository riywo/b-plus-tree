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