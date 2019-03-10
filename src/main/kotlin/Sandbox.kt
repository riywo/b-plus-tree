package com.riywo.ninja.bptree

import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.*
import org.apache.avro.specific.*
import org.apache.avro.io.*

import LeafNodePage
import java.io.ByteArrayOutputStream

val keySchema = SchemaBuilder.builder().record("key").fields()
    .name("a").type().stringType().noDefault()
    .endRecord()
val valueSchema = SchemaBuilder.builder().record("value").fields()
    .name("b").orderIgnore().type().stringType().noDefault()
    .endRecord()

val table = Table(keySchema, valueSchema)

fun ByteArray.toHexString() = joinToString(":") { String.format("%02x", it) }

fun main() {
    val leafNode = LeafNode(table)
    println(leafNode.keys.map{ it.toByteArray().toHexString() })
    println(leafNode.dump().toHexString())

    val key = GenericData.Record(keySchema)
    key.put("a", "a")
    val value = GenericData.Record(valueSchema)
    value.put("b", "b")
    println(leafNode.get(table.key.write(key)))

    leafNode.put(table.key.write(key), table.value.write(value))
    println(leafNode.keys.map{ it.toByteArray().toHexString() })
    println(leafNode.get(table.key.write(key))?.toByteArray()?.toHexString())
    println(leafNode.dump().toHexString())

    val key2 = GenericData.Record(keySchema)
    key2.put("a", "c")
    val value2 = GenericData.Record(valueSchema)
    value2.put("b", "c")

    leafNode.put(table.key.write(key2), table.value.write(value2))
    println(leafNode.keys.map{ it.toByteArray().toHexString() })
    println(leafNode.get(table.key.write(key))?.toByteArray()?.toHexString())
    println(leafNode.dump().toHexString())

    val leafNode2 = LeafNode(table)
    leafNode2.load(leafNode.dump())
    println(leafNode2.keys.map{ it.toByteArray().toHexString() })
}
