package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*
import java.nio.ByteBuffer

class TestTree {
    private val compare: KeyCompare = { a, b ->
        a[0].compareTo(b[0])
    }
    private var pageManager = PageManager()
    private var tree = Tree(pageManager, compare, pageManager.create(NodeType.LeafNode, mutableListOf()))
    private val record = Record(byteArrayOf(1), byteArrayOf(1))

    @BeforeEach
    fun init() {
        pageManager = PageManager()
        tree = Tree(pageManager, compare, pageManager.create(NodeType.LeafNode, mutableListOf()))
        tree.put(record)
    }

    @Test
    fun `get no-root`() {
        assertThat(tree.get(record.key)).isEqualTo(record)
    }

    @Test
    fun `update no-root`() {
        val newRecord = Record(record.key, byteArrayOf(2))
        tree.put(newRecord)
        assertThat(tree.get(record.key)).isEqualTo(newRecord)
    }

    @Test
    fun `insert ordered`() {
        val records = (2..3000).map { Record(byteArrayOf(it.toByte()), ByteBuffer.allocate(MAX_PAGE_SIZE/50)) }
        for (newRecord in records) {
            tree.put(newRecord)
        }
        val scanned = tree.scan(records.first().key, records.last().key).toList()
        assertThat(scanned).isEqualTo(records)
        val scannedReversed = tree.scan(records.last().key, records.first().key).toList()
        assertThat(scannedReversed).isEqualTo(records.reversed())
    }

    @Test
    fun `insert reversed ordered`() {
        val records = (2..3000).map { Record(byteArrayOf(it.toByte()), ByteBuffer.allocate(MAX_PAGE_SIZE/50)) }
        for (newRecord in records.reversed()) {
            tree.put(newRecord)
        }
        val scanned = tree.scan(records.first().key, records.last().key).toList()
        assertThat(scanned).isEqualTo(records)
        val scannedReversed = tree.scan(records.last().key, records.first().key).toList()
        assertThat(scannedReversed).isEqualTo(records.reversed())
    }

    @Test
    fun `insert shuffled ordered`() {
        val records = (2..3000).map { Record(byteArrayOf(it.toByte()), ByteBuffer.allocate(MAX_PAGE_SIZE/50)) }
        for (newRecord in records.shuffled()) {
            tree.put(newRecord)
        }
        val scanned = tree.scan(records.first().key, records.last().key).toList()
        assertThat(scanned).isEqualTo(records)
        val scannedReversed = tree.scan(records.last().key, records.first().key).toList()
        assertThat(scannedReversed).isEqualTo(records.reversed())
    }
}