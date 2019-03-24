package com.riywo.ninja.bptree

import NodeType
import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*

class TestLeafNode {
    private val compare: KeyCompare = { a, b ->
        a[0].compareTo(b[0])
    }
    private var node = LeafNode(Page.new(1, NodeType.LeafNode, mutableListOf()), compare)
    private val record = Record(byteArrayOf(1), byteArrayOf(1))

    @BeforeEach
    fun init() {
        node = LeafNode(Page.new(1, NodeType.LeafNode, mutableListOf()), compare)
        node.put(record)
        assertThat(node.id).isEqualTo(1)
        assertThat(node.type).isEqualTo(NodeType.LeafNode)
        assertThat(node.previousId).isEqualTo(null)
        assertThat(node.nextId).isEqualTo(null)
        assertThat(node.recordsSize).isEqualTo(1)
        assertThat(node.size).isEqualTo(node.dump().limit())
    }

    @Test
    fun `dump and load`() {
        val nodeLoaded = LeafNode(Page.load(node.dump()), compare)
        assertThat(nodeLoaded.id).isEqualTo(node.id)
        assertThat(nodeLoaded.dump()).isEqualTo(node.dump())
    }

    @Test
    fun `get record`() {
        val found = node.get(record)
        assertThat(found).isEqualTo(record)
        assertThat(node.recordsSize).isEqualTo(1)
    }

    @Test
    fun `get record not found`() {
        val found = node.get(byteArrayOf(2).toByteBuffer())
        assertThat(found).isEqualTo(null)
    }

    @Test
    fun `insert record`() {
        val newRecord = Record(byteArrayOf(2), byteArrayOf(2))
        node.put(newRecord)

        assertThat(node.recordsSize).isEqualTo(2)
        assertThat(node.get(record.key)).isEqualTo(record)
        assertThat(node.get(newRecord.key)).isEqualTo(newRecord)
        assertThat(node.size).isEqualTo(node.dump().limit())
    }

    @Test
    fun `update record`() {
        val newRecord = Record(record.key, byteArrayOf(2))
        node.put(newRecord)

        assertThat(node.recordsSize).isEqualTo(1)
        assertThat(node.get(record.key)).isEqualTo(newRecord)
        assertThat(node.size).isEqualTo(node.dump().limit())
    }

    @Test
    fun `delete record`() {
        node.delete(record)

        assertThat(node.recordsSize).isEqualTo(0)
        assertThat(node.get(record.key)).isEqualTo(null)
        assertThat(node.size).isEqualTo(node.dump().limit())
    }

    @Test
    fun `insert record page full`() {
        val newRecord = Record(byteArrayOf(2), ByteArray(MAX_PAGE_SIZE))

        assertThrows<PageFullException> {
            node.put(newRecord)
        }
        assertThat(node.recordsSize).isEqualTo(2)
        assertThat(node.get(record.key)).isEqualTo(record)
        assertThat(node.get(newRecord.key)).isEqualTo(newRecord)
        assertThat(node.size).isEqualTo(node.dump().limit())
    }

    @Test
    fun `update record page full`() {
        val newRecord = Record(record.key, ByteArray(MAX_PAGE_SIZE))

        assertThrows<PageFullException> {
            node.put(newRecord)
        }
        assertThat(node.recordsSize).isEqualTo(1)
        assertThat(node.get(record.key)).isEqualTo(newRecord)
        assertThat(node.size).isEqualTo(node.dump().limit())
    }

    @Test
    fun `insert ordered`() {
        val num = 5
        for (i in 2..num) {
            val newRecord = Record(byteArrayOf(i.toByte()), byteArrayOf(i.toByte()))
            node.put(newRecord)
        }
        assertThat(node.records.map{it.key[0].toInt()}.toList()).isEqualTo((1..num).toList())
        assertThat(node.size).isEqualTo(node.dump().limit())
    }

    @Test
    fun `insert reversed`() {
        val num = 5
        for (i in num downTo 2) {
            val newRecord = Record(byteArrayOf(i.toByte()), byteArrayOf(i.toByte()))
            node.put(newRecord)
        }
        assertThat(node.records.map{it.key[0].toInt()}.toList()).isEqualTo((1..num).toList())
        assertThat(node.size).isEqualTo(node.dump().limit())
    }
}