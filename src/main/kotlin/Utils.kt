package com.riywo.ninja.bptree

import org.apache.avro.generic.*

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

