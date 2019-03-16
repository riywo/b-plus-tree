package com.riywo.ninja.bptree

import org.apache.avro.generic.GenericRecord

class LeafNode(table: Table, page: AvroPage) : Node(table.key, table.record, page) {
    fun get(key: GenericRecord): GenericRecord? {
        val result = find(key)
        return when (result) {
            is FindResult.ExactMatch -> createRecord(result.byteBuffer)
            else -> null
        }
    }

    fun put(record: GenericRecord) {
        val byteBuffer = encodeRecord(record)
        val result = find(record)
        when(result) { // TODO merge new and old record
            is FindResult.ExactMatch -> page.update(result.index, byteBuffer)
            is FindResult.LeftMatch -> page.insert(result.index, byteBuffer)
            is FindResult.RightMatch -> page.insert(result.index, byteBuffer)
        }
    }

    fun delete(key: GenericRecord) {
        val result = find(key)
        if (result is FindResult.ExactMatch) page.delete(result.index)
    }
}
