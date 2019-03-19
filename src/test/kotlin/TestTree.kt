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
        val records = (2..3000).map { createRecord(it, "a".repeat(MAX_PAGE_SIZE/50)) }
        for (newRecord in records) {
            tree.put(newRecord)
        }
        val scanned = tree.scan(createKey(2), createKey(3000)).toList()
        assertThat(scanned).isEqualTo(records)
        val scannedReversed = tree.scan(createKey(3000), createKey(2)).toList()
        assertThat(scannedReversed).isEqualTo(records.reversed())
    }

    @Test
    fun `insert reversed ordered`() {
        val records = (2..3000).map { createRecord(it, "a".repeat(MAX_PAGE_SIZE/50)) }
        for (newRecord in records.reversed()) {
            tree.put(newRecord)
        }
        val scanned = tree.scan(createKey(2), createKey(3000)).toList()
        assertThat(scanned).isEqualTo(records)
        val scannedReversed = tree.scan(createKey(3000), createKey(2)).toList()
        assertThat(scannedReversed).isEqualTo(records.reversed())
    }

    @Test
    fun `insert shuffled ordered`() {
        val records = (2..3000).map { createRecord(it, "a".repeat(MAX_PAGE_SIZE/50)) }
        for (newRecord in records.shuffled()) {
            tree.put(newRecord)
        }
        val scanned = tree.scan(createKey(2), createKey(3000)).toList()
        assertThat(scanned).isEqualTo(records)
        val scannedReversed = tree.scan(createKey(3000), createKey(2)).toList()
        assertThat(scannedReversed).isEqualTo(records.reversed())
        tree.debug()
    }
}