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
    private var node = InternalNode(table, AvroPage.new(1))

    @BeforeEach
    fun init() {
        node = InternalNode(table, AvroPage.new(1))
        assertThat(node.id).isEqualTo(1)
        assertThat(node.previousId).isEqualTo(null)
        assertThat(node.nextId).isEqualTo(null)
        assertThat(node.getRecords().size).isEqualTo(0)
        assertThat(node.size).isEqualTo(node.dump().limit())
    }

    @Test
    fun `dump and load`() {
        val nodeLoaded = LeafNode(table, AvroPage.load(node.dump()))
        assertThat(nodeLoaded.id).isEqualTo(node.id)
        assertThat(nodeLoaded.dump()).isEqualTo(node.dump())
    }
}