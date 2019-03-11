package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.junit.jupiter.params.*
import org.junit.jupiter.params.provider.*
import org.assertj.core.api.Assertions.*
import java.util.stream.*

import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import java.io.*

class TestAvroRecord {
    @Nested
    class IO {
        private val schema = SchemaBuilder.record("test").fields()
            .name("f1").type().stringType().noDefault()
            .endRecord()

        private val io = AvroRecord.IO(schema)

        data class TestData(
            val f1: String,
            val bytes: ByteArray
        )

        private fun testDataProvider() = Stream.of(
            TestData("foo", byteArrayOf(6, 102, 111, 111))
        )

        @ParameterizedTest
        @MethodSource("testDataProvider")
        fun `write to stream`(data: TestData) {
            val output = ByteArrayOutputStream()
            val record = GenericData.Record(schema)
            record.put("f1", data.f1)
            io.encode(record, output)
            assertThat(output.toByteArray()).isEqualTo(data.bytes)
        }

        @ParameterizedTest
        @MethodSource("testDataProvider")
        fun `read from stream`(data: TestData) {
            val input = ByteArrayInputStream(data.bytes)
            val record = GenericData.Record(schema)
            io.decode(record, input)
            assertThat(record.get("f1").toString()).isEqualTo(data.f1)
        }
    }
}