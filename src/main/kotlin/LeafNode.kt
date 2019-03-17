package com.riywo.ninja.bptree

open class LeafNode(table: Table, page: Page) : Node(table, page) {
    fun get(key: Table.Key): Table.Record? {
        val result = find(key)
        return when (result) {
            is FindResult.ExactMatch -> table.createRecord(result.byteBuffer)
            else -> null
        }
    }

    fun put(record: Table.Record) {
        val byteBuffer = record.toByteBuffer()
        val result = find(record)
        when(result) {
            is FindResult.ExactMatch -> page.update(result.index, byteBuffer) // TODO merge new and old
            is FindResult.FirstGraterThanMatch -> page.insert(result.index, byteBuffer)
            null -> page.insert(0, byteBuffer)
        }
    }

    fun delete(key: Table.Key) {
        val result = find(key)
        if (result is FindResult.ExactMatch) page.delete(result.index)
    }
}
