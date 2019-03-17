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
    private var tree = Tree(table, pageManager, pageManager.create(NodeType.LeafNode))

    private fun createRecord(key: Int): Table.Record {
        val record = table.Record()
        record.put("key", key)
        record.put("value", "$key")
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
        tree = Tree(table, pageManager, pageManager.create(NodeType.LeafNode))
        val record = createRecord(1)
        tree.put(record)
    }

    @Test
    fun `get a record`() {
//        assertThat(tree.get(createKey(1))).isEqualTo(createRecord(1))
    }
}