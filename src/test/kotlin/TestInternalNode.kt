package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*

class TestInternalNode {
    private val compare: KeyCompare = { a, b ->
        a[0].compareTo(b[0])
    }
    private var node = createInternalNode()

    private fun createInternalNode(leftFirst: Boolean = true): InternalNode {
        val leftLeafNode = createLeafNode(2)
        val rightLeafNode = createLeafNode(4)
        val node = InternalNode(Page.new(99, NodeType.InternalNode, mutableListOf()), compare)
        if (leftFirst) {
            node.addChildNode(leftLeafNode)
            node.addChildNode(rightLeafNode)
        } else {
            node.addChildNode(rightLeafNode)
            node.addChildNode(leftLeafNode)
        }
        return node
    }

    private fun createLeafNode(id: Int): LeafNode {
        val node = LeafNode(Page.new(id, NodeType.LeafNode, mutableListOf()), compare)
        val key = byteArrayOf(id.toByte()).toByteBuffer()
        val value = byteArrayOf(id.toByte()).toByteBuffer()
        node.put(key, value)
        return node
    }

    private fun getKeys(): List<Int> {
        return node.records.map{it.key[0].toInt()}.toList()
    }

    @BeforeEach
    fun init() {
        node = createInternalNode()
        assertThat(node.id).isEqualTo(99)
        assertThat(node.type).isEqualTo(NodeType.InternalNode)
        assertThat(node.previousId).isEqualTo(null)
        assertThat(node.nextId).isEqualTo(null)
        assertThat(getKeys()).isEqualTo(listOf(2, 4))
        assertThat(node.size).isEqualTo(node.dump().limit())
    }

    @Test
    fun `initialize right first`() {
        node = createInternalNode(false)
        assertThat(node.id).isEqualTo(99)
        assertThat(node.type).isEqualTo(NodeType.InternalNode)
        assertThat(node.previousId).isEqualTo(null)
        assertThat(node.nextId).isEqualTo(null)
        assertThat(getKeys()).isEqualTo(listOf(2, 4))
        assertThat(node.size).isEqualTo(node.dump().limit())
    }

    @Test
    fun `dump and load`() {
        val nodeLoaded = InternalNode(Page.load(node.dump()), compare)
        assertThat(nodeLoaded.id).isEqualTo(node.id)
        assertThat(nodeLoaded.dump()).isEqualTo(node.dump())
    }

    @Test
    fun `find child id`() {
        assertThat(node.findChildPageId(byteArrayOf(0).toByteBuffer())).isEqualTo(2)
        assertThat(node.findChildPageId(byteArrayOf(1).toByteBuffer())).isEqualTo(2)
        assertThat(node.findChildPageId(byteArrayOf(2).toByteBuffer())).isEqualTo(2)
        assertThat(node.findChildPageId(byteArrayOf(3).toByteBuffer())).isEqualTo(2)
        assertThat(node.findChildPageId(byteArrayOf(4).toByteBuffer())).isEqualTo(4)
        assertThat(node.findChildPageId(byteArrayOf(5).toByteBuffer())).isEqualTo(4)
        assertThat(node.findChildPageId(byteArrayOf(6).toByteBuffer())).isEqualTo(4)
    }

    @Test
    fun `add children`() {
        val leafNode1 = createLeafNode(1)
        val leafNode3 = createLeafNode(3)
        val leafNode5 = createLeafNode(5)
        node.addChildNode(leafNode1)
        node.addChildNode(leafNode3)
        node.addChildNode(leafNode5)

        assertThat(getKeys()).isEqualTo(listOf(1, 2, 3, 4, 5))
        assertThat(node.findChildPageId(byteArrayOf(0).toByteBuffer())).isEqualTo(1)
        assertThat(node.findChildPageId(byteArrayOf(1).toByteBuffer())).isEqualTo(1)
        assertThat(node.findChildPageId(byteArrayOf(2).toByteBuffer())).isEqualTo(2)
        assertThat(node.findChildPageId(byteArrayOf(3).toByteBuffer())).isEqualTo(3)
        assertThat(node.findChildPageId(byteArrayOf(4).toByteBuffer())).isEqualTo(4)
        assertThat(node.findChildPageId(byteArrayOf(5).toByteBuffer())).isEqualTo(5)
        assertThat(node.findChildPageId(byteArrayOf(6).toByteBuffer())).isEqualTo(5)
    }
}