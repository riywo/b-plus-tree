package com.riywo.ninja.bptree

open class LeafNode(keyIO: AvroGenericRecord.IO, recordIO: AvroGenericRecord.IO, page: Page)
    : Node(keyIO, recordIO, page) {

    constructor(table: Table, page: Page) : this(table.key, table.record, page)

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
        when(result) {
            is FindResult.ExactMatch -> page.update(result.index, byteBuffer) // TODO merge new and old
            is FindResult.FirstGraterThanMatch -> page.insert(result.index, byteBuffer)
            null -> page.insert(0, byteBuffer)
        }
    }

    fun delete(key: AvroGenericRecord) {
        val result = find(key)
        if (result is FindResult.ExactMatch) page.delete(result.index)
    }
}
