package com.riywo.ninja.bptree

import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.*
import org.apache.avro.specific.*
import org.apache.avro.io.*

import LeafNodePage
import java.io.ByteArrayOutputStream

fun LeafNode.records(): List<Table.Record> {
    return pageRecords().map {
        val record = table.Record()
        record.load(it)
        record
    }
}

fun LeafNode.get(record: GenericRecord): Table.Record? {
    val keyByteBuffer = table.key.encode(record)
    val byteBuffer = get(keyByteBuffer)
    return if (byteBuffer == null) {
        null
    } else {
        val found = table.Record()
        found.load(byteBuffer)
        found
    }
}

fun LeafNode.put(record: GenericRecord) {
    val keyByteBuffer = table.key.encode(record)
    val recordByteBuffer = table.record.encode(record)
    put(keyByteBuffer, recordByteBuffer)
}

fun LeafNode.delete(record: GenericRecord) {
    val keyByteBuffer = table.key.encode(record)
    delete(keyByteBuffer)
}

fun main() {
    val schema = SchemaBuilder.builder().record("foo").fields()
        .name("a").type().stringType().noDefault()
        .name("b").orderIgnore().type().stringType().noDefault()
        .endRecord()

    val table = Table(schema)

    val leafNode = LeafNode.new(table)
    println(leafNode.records())
    println(leafNode.dump().toHexString())

    val record = table.Record()
    record.put("a", "a")
    record.put("b", "b")
    println(leafNode.get(record))
    leafNode.put(record)
    println(leafNode.records())
    println(leafNode.get(record))
    println(leafNode.dump().toHexString())

    val record2 = table.Record()
    record2.put("a", "c")
    record2.put("b", "c")
    leafNode.put(record2)
    println(leafNode.records())
    println(leafNode.get(record2))
    println(leafNode.dump().toHexString())

    val record3 = table.Record()
    record3.put("a", "a")
    record3.put("b", "c")
    leafNode.put(record3)
    println(leafNode.records())
    println(leafNode.get(record3))
    println(leafNode.dump().toHexString())

    val leafNode2 = LeafNode.load(table, leafNode.dump())
    println(leafNode2.records())
}
