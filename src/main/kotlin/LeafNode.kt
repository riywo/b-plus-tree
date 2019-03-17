package com.riywo.ninja.bptree

class LeafNode(table: Table, page: Page) : Node(table.key, table.record, page) {
    fun get(key: AvroGenericRecord): AvroGenericRecord? {
        val result = find(key)
        return when (result) {
            is FindResult.ExactMatch -> createRecord(result.byteBuffer)
            else -> null
        }
    }

    fun put(record: AvroGenericRecord) {
        val byteBuffer = record.toByteBuffer()
        val result = find(record)
        when(result) { // TODO merge new and old record
            is FindResult.ExactMatch -> page.update(result.index, byteBuffer)
            is FindResult.FirstGraterThanMatch -> page.insert(result.index, byteBuffer)
            null -> page.insert(0, byteBuffer)
        }
    }

    fun delete(key: AvroGenericRecord) {
        val result = find(key)
        if (result is FindResult.ExactMatch) page.delete(result.index)
    }
}
