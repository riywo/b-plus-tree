package com.riywo.ninja.bptree

import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.*
import org.apache.avro.specific.*
import org.apache.avro.io.*

import LeafNodePage
import java.io.ByteArrayOutputStream

val schema = SchemaBuilder.builder().record("foo").fields()
    .name("a").type().stringType().noDefault()
    .name("b").orderIgnore().type().stringType().noDefault()
    .endRecord()

val table = Table(schema)

fun main() {
    val leafNode = LeafNode.new(table)
    println(leafNode.records)
    println(leafNode.dump().toHexString())

    val key = table.Key()
    key.put("a", "a")
    val value = table.Value()
    value.put("b", "b")
    println(leafNode.get(key))

    leafNode.put(key, value)
    println(leafNode.records)
    println(leafNode.get(key))
    println(leafNode.dump().toHexString())

    val key2 = GenericData.Record(table.key.schema)
    key2.put("a", "c")
    val value2 = GenericData.Record(table.value.schema)
    value2.put("b", "c")

    leafNode.put(key2, value2)
    println(leafNode.records)
    println(leafNode.get(key))
    println(leafNode.dump().toHexString())

    val leafNode2 = LeafNode.load(table, leafNode.dump())
    println(leafNode2.records)
}
