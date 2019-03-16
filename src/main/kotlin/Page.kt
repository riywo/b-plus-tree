package com.riywo.ninja.bptree

import PageData
import NodeType
import java.nio.ByteBuffer

class Page private constructor(
    private val data: PageData,
    private var byteSize: Int = 0
) {
    companion object {
        fun new(id: Int, nodeType: NodeType): Page {
            val data = createPageData(id, nodeType)
            return Page(data)
        }

        fun load(byteBuffer: ByteBuffer): Page {
            val data = PageData.fromByteBuffer(byteBuffer)
            return Page(data, byteBuffer.limit())
        }
    }

    init {
        if (byteSize == 0) byteSize = dump().limit()
    }

    val id: Int by data
    val nodeType: NodeType by data
    var sentinelId: Int? by data
    var previousId: Int? by data
    var nextId: Int? by data
    val records: List<ByteBuffer> get() = data.getRecords()
    val size: Int get() = byteSize

    fun dump(): ByteBuffer = data.toByteBuffer()

    fun insert(index: Int, byteBuffer: ByteBuffer) {
        val newByteSize = calcPageSize(byteBuffer.toAvroBytesSize(), 1)
        if (newByteSize > MAX_PAGE_SIZE) {
            throw PageFullException("Can't insert record")
        } else {
            data.getRecords().add(index, byteBuffer)
            byteSize = newByteSize
        }
    }

    fun update(index: Int, newByteBuffer: ByteBuffer) {
        val oldByteBuffer = data.getRecords()[index]
        val newByteSize = calcPageSize(newByteBuffer.toAvroBytesSize() - oldByteBuffer.toAvroBytesSize())
        if (newByteSize > MAX_PAGE_SIZE) {
            throw PageFullException("Can't update record")
        } else {
            data.getRecords()[index] = newByteBuffer
            byteSize = newByteSize
        }
    }

    fun delete(index: Int) {
        val byteBuffer = data.getRecords()[index]
        val newByteSize = calcPageSize(-byteBuffer.toAvroBytesSize(), -1)
        data.getRecords().removeAt(index)
        byteSize = newByteSize
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
