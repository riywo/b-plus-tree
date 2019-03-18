package com.riywo.ninja.bptree

import NodeType
import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*

import org.apache.avro.SchemaBuilder

class TestLeafNode {
    private val schema = SchemaBuilder.builder().record("foo").fields()
        .name("key").type().intType().noDefault()
        .name("value").orderIgnore().type().stringType().noDefault()
        .endRecord()
    private val table = Table(schema)
    private var node = LeafNode(table, Page.new(1, NodeType.LeafNode, mutableListOf()))
    private val record = table.Record()
    private val key = table.Key()

    init {
        record.put("key", 1)
        record.put("value", "a")
        key.put("key", 1)
    }

    @BeforeEach
    fun init() {
        node = LeafNode(table, Page.new(1, NodeType.LeafNode, mutableListOf()))
        node.put(record)
        assertThat(node.id).isEqualTo(1)
        assertThat(node.type).isEqualTo(NodeType.LeafNode)
        assertThat(node.previousId).isEqualTo(null)
        assertThat(node.nextId).isEqualTo(null)
        assertThat(node.records.size).isEqualTo(1)
        assertThat(node.size).isEqualTo(node.dump().limit())
    }

    @Test
    fun `dump and load`() {
        val nodeLoaded = LeafNode(table, Page.load(node.dump()))
        assertThat(nodeLoaded.id).isEqualTo(node.id)
        assertThat(nodeLoaded.dump()).isEqualTo(node.dump())
    }

    @Test
    fun `get record`() {
        val found = node.get(key)
        assertThat(found).isEqualTo(record)
        assertThat(node.records.size).isEqualTo(1)
    }

    @Test
    fun `insert record`() {
        val recordInserted = table.Record()
        recordInserted.put("key", 2)
        recordInserted.put("value", "b")
        val keyInserted = table.Key()
        keyInserted.put("key", 2)
        node.put(recordInserted)

        assertThat(node.records.size).isEqualTo(2)
        assertThat(node.get(key)).isEqualTo(record)
        assertThat(node.get(keyInserted)).isEqualTo(recordInserted)
        assertThat(node.size).isEqualTo(node.dump().limit())
    }

    @Test
    fun `update record`() {
        val recordUpdated = table.Record()
        recordUpdated.put("key", 1)
        recordUpdated.put("value", "b")
        node.put(recordUpdated)

        assertThat(node.records.size).isEqualTo(1)
        assertThat(node.get(key)).isEqualTo(recordUpdated)
        assertThat(node.size).isEqualTo(node.dump().limit())
    }

    @Test
    fun `delete record`() {
        node.delete(key)

        assertThat(node.records.size).isEqualTo(0)
        assertThat(node.get(key)).isEqualTo(null)
        assertThat(node.size).isEqualTo(node.dump().limit())
    }

    @Test
    fun `insert record page full`() {
        val newRecord = table.Record()
        newRecord.put("key", 2)
        newRecord.put("value", "a".repeat(MAX_PAGE_SIZE))
        assertThrows<PageFullException> {
            node.put(newRecord)
        }
        val newKey = table.Key()
        newKey.put("key", 2)
        assertThat(node.records.size).isEqualTo(2)
        assertThat(node.get(key)).isEqualTo(record)
        assertThat(node.get(newKey)).isEqualTo(newRecord)
        assertThat(node.size).isEqualTo(node.dump().limit())
    }

    @Test
    fun `update record page full`() {
        val newRecord = table.Record()
        newRecord.put("key", 1)
        newRecord.put("value", "a".repeat(MAX_PAGE_SIZE))
        assertThrows<PageFullException> {
            node.put(newRecord)
        }
        assertThat(node.records.size).isEqualTo(1)
        assertThat(node.get(key)).isEqualTo(newRecord)
        assertThat(node.size).isEqualTo(node.dump().limit())
    }

    @Test
    fun `insert ordered`() {
        val num = 5
        for (i in 2..num) {
            val newRecord = table.Record()
            newRecord.put("key", i)
            newRecord.put("value", "$i")
            node.put(newRecord)
        }
        assertThat(node.records.map{table.createRecord(it).get("key")}).isEqualTo((1..num).toList())
        assertThat(node.size).isEqualTo(node.dump().limit())
    }

    @Test
    fun `insert reversed`() {
        val num = 5
        for (i in num downTo 2) {
            val newRecord = table.Record()
            newRecord.put("key", i)
            newRecord.put("value", "$i")
            node.put(newRecord)
        }
        assertThat(node.records.map{table.createRecord(it).get("key")}).isEqualTo((1..num).toList())
        assertThat(node.size).isEqualTo(node.dump().limit())
    }
}