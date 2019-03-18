package com.riywo.ninja.bptree

import java.lang.Exception
import java.nio.ByteBuffer

open class InternalNode(table: Table, page: Page) : LeafNode(table, page) {
    fun findChildPageId(key: AvroGenericRecord): Int {
        val result = find(key)
        return when (result) {
            is FindResult.ExactMatch -> getChildPageId(result.byteBuffer)
            is FindResult.FirstGraterThanMatch -> {
                if (result.index == 0 && previousId != null)
                    throw Exception() // TODO
                val index = if (result.index == 0) 0 else result.index-1
                getChildPageId(page.records[index])
            }
            null -> throw Exception() // TODO
        }
    }

    fun addChildNode(node: Node) {
        val record = table.createInternal(node.minRecord)
        record.childPageId = node.id
        val result = find(record)
        when (result) {
            is FindResult.FirstGraterThanMatch -> page.insert(result.index, record.toByteBuffer())
            null -> page.insert(0, record.toByteBuffer())
            else -> throw Exception() // TODO
        }
    }

    private fun getChildPageId(byteBuffer: ByteBuffer): Int {
        return table.createInternal(byteBuffer).childPageId
    }
}
