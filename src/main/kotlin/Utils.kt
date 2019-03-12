package com.riywo.ninja.bptree

import org.apache.avro.generic.*

fun LeafNode.records(table: Table): List<Table.Record> {
    return records().map {
        val record = table.Record()
        record.load(it)
        record
    }
}

fun LeafNode.get(table: Table, record: GenericRecord): Table.Record? {
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

fun LeafNode.put(table: Table, record: GenericRecord) {
    val keyByteBuffer = table.key.encode(record)
    val recordByteBuffer = table.record.encode(record)
    put(keyByteBuffer, recordByteBuffer)
}

fun LeafNode.delete(table: Table, record: GenericRecord) {
    val keyByteBuffer = table.key.encode(record)
    delete(keyByteBuffer)
}

