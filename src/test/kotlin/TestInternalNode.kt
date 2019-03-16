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

    private fun createInternalNode(): InternalNode {
        val leftLeafNode = createLeafNode(0, 1)
        val rightLeafNode = createLeafNode(1, 2)
        val node = InternalNode(table, Page.new(2, NodeType.InternalNode))
        node.initialize(leftLeafNode, rightLeafNode)
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

    @BeforeEach
    fun init() {
        node = createInternalNode()
        assertThat(node.id).isEqualTo(2)
        assertThat(node.type).isEqualTo(NodeType.InternalNode)
        assertThat(node.previousId).isEqualTo(null)
        assertThat(node.nextId).isEqualTo(null)
        assertThat(node.records.size).isEqualTo(1)
        assertThat(table.createKey(node.records.first()).get("key")).isEqualTo(1)
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
        assertThat(node.findChildPageId(createRecord(0))).isEqualTo(0)
        assertThat(node.findChildPageId(createRecord(1))).isEqualTo(0)
        assertThat(node.findChildPageId(createRecord(2))).isEqualTo(1)
        assertThat(node.findChildPageId(createRecord(3))).isEqualTo(1)
    }
}