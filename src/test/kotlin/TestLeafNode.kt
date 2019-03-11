package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*

import org.apache.avro.SchemaBuilder

class TestLeafNode {
    private val schema = SchemaBuilder.builder().record("foo").fields()
        .name("key").type().stringType().noDefault()
        .name("value").orderIgnore().type().stringType().noDefault()
        .endRecord()
    private val table = Table(schema)

    private var leafNode: LeafNode = LeafNode.new(table)
    private val record = table.Record()

    init {
        record.put("key", "1")
        record.put("value", "a")
        leafNode.put(record)
    }

    @BeforeEach
    fun init() {
        leafNode = LeafNode.new(table)
        leafNode.put(record)
        assertThat(leafNode.records().size).isEqualTo(1)
    }

    @Test
    fun `dump and load`() {
        val leafNodeLoaded = LeafNode.load(table, leafNode.dump())
        assertThat(leafNodeLoaded).isEqualTo(leafNode)
    }

    @Test
    fun `get record`() {
        val found = leafNode.get(record)
        assertThat(found).isEqualTo(record)
    }

    @Test
    fun `insert record`() {
        val recordInserted = table.Record()
        recordInserted.put("key", "2")
        recordInserted.put("value", "b")
        leafNode.put(recordInserted)

        assertThat(leafNode.records().size).isEqualTo(2)
        assertThat(leafNode.get(record)).isEqualTo(record)
        assertThat(leafNode.get(recordInserted)).isEqualTo(recordInserted)
    }

    @Test
    fun `update record`() {
        val recordUpdated = table.Record()
        recordUpdated.put("key", "1")
        recordUpdated.put("value", "b")
        leafNode.put(recordUpdated)

        assertThat(leafNode.records().size).isEqualTo(1)
        assertThat(leafNode.get(record)).isEqualTo(recordUpdated)
        assertThat(leafNode.get(recordUpdated)).isEqualTo(recordUpdated)
    }

    @Test
    fun `delete record`() {
        leafNode.delete(record)

        assertThat(leafNode.records().size).isEqualTo(0)
        assertThat(leafNode.get(record)).isEqualTo(null)
    }
}