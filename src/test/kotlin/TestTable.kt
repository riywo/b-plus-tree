package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*

import org.apache.avro.SchemaBuilder

class TestTable {
    private val intKeySchema = SchemaBuilder.builder().record("key").fields()
        .name("key").type().intType().noDefault().endRecord()
    private val stringKeySchema = SchemaBuilder.builder().record("key").fields()
        .name("key").type().stringType().noDefault().endRecord()
    private val intStringKeySchema = SchemaBuilder.builder().record("key").fields()
        .name("key1").type().intType().noDefault()
        .name("key2").type().stringType().noDefault().endRecord()
    private val valueSchema = SchemaBuilder.builder().record("value").fields()
        .name("value").type().stringType().noDefault().endRecord()

    @Test
    fun `minimumKey int`() {
        val table = Table(intKeySchema, valueSchema)
        assertThat(table.minimumKey).isEqualTo(table.createKey("{\"key\": ${Int.MIN_VALUE}}"))
    }

    @Test
    fun `minimumKey string`() {
        val table = Table(stringKeySchema, valueSchema)
        assertThat(table.minimumKey).isEqualTo(table.createKey("{\"key\": \"\"}"))
    }

    @Test
    fun `minimumKey int and string`() {
        val table = Table(intStringKeySchema, valueSchema)
        assertThat(table.minimumKey).isEqualTo(table.createKey("{\"key1\": ${Int.MIN_VALUE}, \"key2\": \"\"}"))
    }

}