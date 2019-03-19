package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*

import org.apache.avro.SchemaBuilder

class TestTree {
    private val schema = SchemaBuilder.builder().record("foo").fields()
        .name("key").type().intType().noDefault()
        .name("value").orderIgnore().type().stringType().noDefault()
        .endRecord()
    private val table = Table(schema)
    private var pageManager = PageManager()
    private var tree = Tree(table, pageManager, pageManager.create(NodeType.LeafNode, mutableListOf()))

    private fun createRecord(key: Int, value: String = "$key"): Table.Record {
        val record = table.Record()
        record.put("key", key)
        record.put("value", value)
        return record
    }

    private fun createKey(key: Int): Table.Key {
        val record = table.Key()
        record.put("key", key)
        return record
    }

    @BeforeEach
    fun init() {
        pageManager = PageManager()
        tree = Tree(table, pageManager, pageManager.create(NodeType.LeafNode, mutableListOf()))
        val record = createRecord(1)
        tree.put(record)
    }

    @Test
    fun `get no-root`() {
        assertThat(tree.get(createKey(1))).isEqualTo(createRecord(1))
    }

    @Test
    fun `update no-root`() {
        val newRecord = createRecord(1, "2")
        tree.put(newRecord)
        assertThat(tree.get(createKey(1))).isEqualTo(createRecord(1))
    }

    @Test
    fun `insert ordered`() {
        for (i in 2..3000) {
            val newRecord = createRecord(i, "a".repeat(MAX_PAGE_SIZE/50))
            tree.put(newRecord)
            assertThat(tree.get(createKey(i))).isEqualTo(newRecord)
        }
        tree.debug()
    }

    @Test
    fun `insert reverse ordered`() {
        for (i in 3000 downTo 2) {
            val newRecord = createRecord(i, "a".repeat(MAX_PAGE_SIZE/50))
            tree.put(newRecord)
            assertThat(tree.get(createKey(i))).isEqualTo(newRecord)
        }
        tree.debug()
    }
}