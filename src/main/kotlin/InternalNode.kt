package com.riywo.ninja.bptree

import java.lang.Exception
import java.nio.ByteBuffer

class InternalNode(table: Table, page: Page) : Node(table.key, table.internal, page) {
    fun findChildPageId(key: AvroGenericRecord): Int? {
        val result = find(key)
        return when (result) {
            is FindResult.ExactMatch -> getChildPageId(result.byteBuffer)
            is FindResult.FirstGraterThanMatch -> {
                if (result.index == 0 && previousId != null)
                    throw Exception() // TODO
                val index = if (result.index == 0) 0 else result.index-1
                getChildPageId(page.records[index])
            }
            null -> null
        }
    }

    fun addChildNode(node: Node) {
        val record = createInternalRecord(node.minRecord, node.id)
        val result = find(record)
        when (result) {
            is FindResult.FirstGraterThanMatch -> page.insert(result.index, record.toByteBuffer())
            null -> page.insert(0, record.toByteBuffer())
            else -> throw Exception() // TODO
        }
    }

    private fun getChildPageId(byteBuffer: ByteBuffer): Int {
        return createRecord(byteBuffer).get(INTERNAL_ID_FIELD_NAME) as Int
    }

    private fun createInternalRecord(byteBuffer: ByteBuffer, childId: Int): AvroGenericRecord {
        val internal = createRecord(byteBuffer)
        internal.put(INTERNAL_ID_FIELD_NAME, childId)
        return internal
    }
}
