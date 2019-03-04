package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*

import org.apache.avro.SchemaBuilder

class TestTable {
    private val schema = SchemaBuilder.record("test_record").fields()
        .name("k1").type().intType().noDefault()
        .name("k2").type().intType().noDefault()
        .name("f1").orderIgnore().type().nullable().stringType().noDefault()
        .endRecord()
    private val keySchema = SchemaBuilder.record("test_record_key").fields()
        .name("k1").type().intType().noDefault()
        .name("k2").type().intType().noDefault()
        .endRecord()
    private val table = Table(schema)

    @Test
    fun `valid keySchema`() {
        assertThat(table.keySchema).isEqualToIgnoringGivenFields(keySchema, "name")
    }

    @Nested
    inner class TestRecord {
        private val record = table.Record()

        @Test
        fun `valid schema`() {
            assertThat(record.schema).isEqualTo(schema)
        }
    }

    @Nested
    inner class TestKey {
        private val key = table.Key()

        @Test
        fun `valid schema`() {
            assertThat(key.schema).isEqualToIgnoringGivenFields(keySchema, "name")
        }
    }
}