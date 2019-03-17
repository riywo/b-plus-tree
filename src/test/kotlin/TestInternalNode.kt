package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*

import org.apache.avro.SchemaBuilder

class TestInternalNode {
    private val schema = SchemaBuilder.builder().record("foo").fields()
        .name("key").type().intType().noDefault()
        .name("value").orderIgnore().type().stringType().noDefault()
        .endRecord()
    private val table = Table(schema)
    private var node = createInternalNode()

    private fun createInternalNode(leftFirst: Boolean = true): InternalNode {
        val leftLeafNode = createLeafNode(2, 2)
        val rightLeafNode = createLeafNode(4, 4)
        val node = InternalNode(table, Page.new(99, NodeType.InternalNode))
        if (leftFirst) {
            node.addChildNode(leftLeafNode)
            node.addChildNode(rightLeafNode)
        } else {
            node.addChildNode(rightLeafNode)
            node.addChildNode(leftLeafNode)
        }
        return node
    }

    private fun createLeafNode(id: Int, key: Int): LeafNode {
        val node = LeafNode(table, Page.new(id, NodeType.LeafNode))
        val record = createRecord(key)
        node.put(record)
        return node
    }

    private fun createRecord(key: Int): Table.Record {
        val record = table.Record()
        record.put("key", key)
        record.put("value", "$key")
        return record
    }

    private fun getKeys(): List<Int> {
        return node.records.map{table.createKey(it).get("key") as Int}
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
        val nodeLoaded = LeafNode(table, Page.load(node.dump()))
        assertThat(nodeLoaded.id).isEqualTo(node.id)
        assertThat(nodeLoaded.dump()).isEqualTo(node.dump())
    }

    @Test
    fun `find child id`() {
        assertThat(node.findChildPageId(createRecord(0))).isEqualTo(2)
        assertThat(node.findChildPageId(createRecord(1))).isEqualTo(2)
        assertThat(node.findChildPageId(createRecord(2))).isEqualTo(2)
        assertThat(node.findChildPageId(createRecord(3))).isEqualTo(2)
        assertThat(node.findChildPageId(createRecord(4))).isEqualTo(4)
        assertThat(node.findChildPageId(createRecord(5))).isEqualTo(4)
        assertThat(node.findChildPageId(createRecord(6))).isEqualTo(4)
    }

    @Test
    fun `add children`() {
        val leafNode1 = createLeafNode(1, 1)
        val leafNode3 = createLeafNode(3, 3)
        val leafNode5 = createLeafNode(5, 5)
        node.addChildNode(leafNode1)
        node.addChildNode(leafNode3)
        node.addChildNode(leafNode5)

        assertThat(getKeys()).isEqualTo(listOf(1,2,3,4,5))
        assertThat(node.findChildPageId(createRecord(0))).isEqualTo(1)
        assertThat(node.findChildPageId(createRecord(1))).isEqualTo(1)
        assertThat(node.findChildPageId(createRecord(2))).isEqualTo(2)
        assertThat(node.findChildPageId(createRecord(3))).isEqualTo(3)
        assertThat(node.findChildPageId(createRecord(4))).isEqualTo(4)
        assertThat(node.findChildPageId(createRecord(5))).isEqualTo(5)
        assertThat(node.findChildPageId(createRecord(6))).isEqualTo(5)
    }
}