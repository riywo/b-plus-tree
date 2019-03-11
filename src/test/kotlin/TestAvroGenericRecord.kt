package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.junit.jupiter.params.*
import org.junit.jupiter.params.provider.*
import org.assertj.core.api.Assertions.*
import java.util.stream.*

import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import java.io.*

class TestAvroGenericRecord {
    @Nested
    class IO {
        private val schema = SchemaBuilder.record("test").fields()
            .name("f1").type().stringType().noDefault()
            .endRecord()

        private val io = AvroGenericRecord.IO(schema)

        @Test
        fun `encode and decode`() {
            val record1 = AvroGenericRecord(io)
            record1.put("f1", "aaa")
            val encoded = io.encode(record1)
            val record2 = AvroGenericRecord(io)
            record2.load(encoded)
            assertThat(record2).isEqualTo(record1)

        }
    }
}