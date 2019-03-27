package com.riywo.ninja.bptree

import PageData
import NodeType
import KeyValue
import java.nio.ByteBuffer

private val logger = mu.KotlinLogging.logger {}

class Page private constructor(
    private val data: PageData
) {
    companion object {
        fun new(id: Int, nodeType: NodeType, initialRecords: MutableList<KeyValue>): Page {
            val data = createPageData(id, nodeType, initialRecords)
            return Page(data)
        }

        fun load(byteBuffer: ByteBuffer): Page {
            val data = PageData.fromByteBuffer(byteBuffer)
            return Page(data)
        }

        val sizeOverhead = Page(emptyPageData).dump().limit()
    }

    val id: Int by data
    var nodeType: NodeType by data
    var previousId: Int? by data
    var nextId: Int? by data
    val records: List<KeyValue> get() = data.getRecords()
    val size: Int get() = byteSize

    private var byteSize = dump().limit()

    fun dump(): ByteBuffer = data.toByteBuffer()

    fun insert(index: Int, keyValue: KeyValue) {
        if (index == 0 && previousId != null) throw PageInsertingMinimumException("")
        val newByteSize = calcPageSize(keyValue.toAvroBytesSize(), 1)
        data.getRecords().add(index, keyValue)
        byteSize = newByteSize
        if (newByteSize > MAX_PAGE_SIZE) throw PageFullException("")
    }

    fun update(index: Int, newKeyValue: KeyValue) {
        val oldKeyValue = data.getRecords()[index]
        val newByteSize = calcPageSize(newKeyValue.toAvroBytesSize() - oldKeyValue.toAvroBytesSize())
        data.getRecords()[index] = newKeyValue
        byteSize = newByteSize
        if (newByteSize > MAX_PAGE_SIZE) throw PageFullException("")
    }

    fun delete(index: Int): KeyValue {
        val keyValue = data.getRecords()[index]
        val newByteSize = calcPageSize(-keyValue.toAvroBytesSize(), -1)
        data.getRecords().removeAt(index)
        byteSize = newByteSize
        return keyValue
    }

    private fun calcPageSize(changingBytes: Int, changingLength: Int = 0): Int {
        return byteSize + changingBytes + calcChangingLengthBytes(changingLength)
    }

    private fun calcChangingLengthBytes(changingLength: Int): Int {
        if (changingLength == 0) return 0
        val newLength = records.size + changingLength
        return newLength.toLengthAvroByteSize() - records.size.toLengthAvroByteSize()
    }
}
