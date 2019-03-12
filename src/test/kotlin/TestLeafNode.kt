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

    private var page: Page = AvroPage.new(table, 0)
    private var leafNode: LeafNode = LeafNode(page)
    private val record = table.Record()

    init {
        record.put("key", "1")
        record.put("value", "a")
        leafNode.put(table, record)
    }

    @BeforeEach
    fun init() {
        page = AvroPage.new(table, 0)
        leafNode = LeafNode(page)
        leafNode.put(table, record)
        assertThat(leafNode.records().size).isEqualTo(1)
        assertThat(leafNode.size()).isEqualTo(leafNode.dump().limit())
    }

    @Test
    fun `dump and load`() {
        val pageLoaded = AvroPage.load(table, leafNode.dump())
        val leafNodeLoaded = LeafNode(pageLoaded)
        assertThat(leafNodeLoaded.id()).isEqualTo(leafNode.id())
        assertThat(leafNodeLoaded.size()).isEqualTo(leafNode.size())
        assertThat(leafNodeLoaded.dump()).isEqualTo(leafNode.dump())
    }

    @Test
    fun `get record`() {
        val found = leafNode.get(table, record)
        assertThat(found).isEqualTo(record)
        assertThat(leafNode.records().size).isEqualTo(1)
    }

    @Test
    fun `insert record`() {
        val recordInserted = table.Record()
        recordInserted.put("key", "2")
        recordInserted.put("value", "b")
        leafNode.put(table, recordInserted)

        assertThat(leafNode.records().size).isEqualTo(2)
        assertThat(leafNode.get(table, record)).isEqualTo(record)
        assertThat(leafNode.get(table, recordInserted)).isEqualTo(recordInserted)
        assertThat(leafNode.size()).isEqualTo(leafNode.dump().limit())
    }

    @Test
    fun `update record`() {
        val recordUpdated = table.Record()
        recordUpdated.put("key", "1")
        recordUpdated.put("value", "b")
        leafNode.put(table, recordUpdated)

        assertThat(leafNode.records().size).isEqualTo(1)
        assertThat(leafNode.get(table, record)).isEqualTo(recordUpdated)
        assertThat(leafNode.get(table, recordUpdated)).isEqualTo(recordUpdated)
        assertThat(leafNode.size()).isEqualTo(leafNode.dump().limit())
    }

    @Test
    fun `delete record`() {
        leafNode.delete(table, record)

        assertThat(leafNode.records().size).isEqualTo(0)
        assertThat(leafNode.get(table, record)).isEqualTo(null)
        assertThat(leafNode.size()).isEqualTo(leafNode.dump().limit())
    }

    @Test
    fun `can't put record`() {
        val newRecord = table.Record()
        newRecord.put("key", "2")
        newRecord.put("value", "a".repeat(MAX_PAGE_SIZE))
        assertThrows<PageFullException> {
            leafNode.put(table, newRecord)
        }
        assertThat(leafNode.records().size).isEqualTo(1)
        assertThat(leafNode.get(table, record)).isEqualTo(record)
        assertThat(leafNode.size()).isEqualTo(leafNode.dump().limit())
    }
}