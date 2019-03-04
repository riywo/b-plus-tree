package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*

import org.apache.avro.SchemaBuilder

class TestAvroRecord {
    private val schema = SchemaBuilder.record("test_record").fields()
        .name("k1").type().intType().noDefault()
        .name("k2").type().intType().noDefault()
        .name("v1").orderIgnore().type().nullable().stringType().noDefault()
        .endRecord()

    private val record = AvroRecord(schema, AvroRecord.IO(schema))

    private val testRecord by lazy {
        val record = AvroRecord(schema, AvroRecord.IO(schema))
        record.put("k1", 1)
        record.put("k2", 2)
        record.put("v1", "foo")
        record
    }
    private val testRecordBytes = byteArrayOf(2, 4, 0, 6, 102, 111, 111)

    @Test
    fun encode() {
        assertThat(testRecord.encode()).isEqualTo(testRecordBytes)
    }

    @Test
    fun decode() {
        record.decode(testRecordBytes)
        assertThat(record).isEqualTo(testRecord)
    }
}