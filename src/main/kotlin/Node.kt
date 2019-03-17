package com.riywo.ninja.bptree

import java.nio.ByteBuffer

abstract class Node(
    protected val table: Table,
    protected val page: Page
) {
    val id get() = page.id
    val type get() = page.nodeType
    val previousId get() = page.previousId
    val nextId get() = page.nextId
    val size get() = page.size
    val records get() = page.records
    val minRecord get() = page.records.first()

    fun dump() = page.dump()

    protected sealed class FindResult {
        data class ExactMatch(val index: Int, val byteBuffer: ByteBuffer) : FindResult()
        data class FirstGraterThanMatch(val index: Int) : FindResult()
    }

    protected fun find(key: AvroGenericRecord): FindResult? {
        if (page.records.isEmpty()) return null
        val keyBytes = table.key.encode(key).toByteArray()
        page.records.forEachIndexed { index, byteBuffer ->
            val bytes = byteBuffer.toByteArray()
            when(table.key.compare(bytes, keyBytes)) {
                0 -> return FindResult.ExactMatch(index, byteBuffer)
                1 -> return FindResult.FirstGraterThanMatch(index)
            }
        }
        return FindResult.FirstGraterThanMatch(page.records.size)
    }
}