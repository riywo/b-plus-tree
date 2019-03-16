package com.riywo.ninja.bptree

import PageData
import java.nio.ByteBuffer

class AvroPage(
    private val data: PageData,
    private var byteSize: Int = 0
) {
    companion object {
        fun new(id: Int): AvroPage {
            val data = createPageData(id)
            return AvroPage(data)
        }

        fun load(byteBuffer: ByteBuffer): AvroPage {
            val data = PageData.fromByteBuffer(byteBuffer)
            return AvroPage(data, byteBuffer.limit())
        }
    }

    init {
        if (byteSize == 0) byteSize = dump().limit()
    }

    val id: Int by data
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
            byteSize = newByteSize
            data.getRecords().add(index, byteBuffer)
        }
    }

    fun update(index: Int, newByteBuffer: ByteBuffer, oldByteBuffer: ByteBuffer) {
        val newByteSize = calcPageSize(newByteBuffer.toAvroBytesSize() - oldByteBuffer.toAvroBytesSize())
        if (newByteSize > MAX_PAGE_SIZE) {
            throw PageFullException("Can't update record")
        } else {
            byteSize = newByteSize
            data.getRecords()[index] = newByteBuffer
        }
    }

    fun delete(index: Int, byteBuffer: ByteBuffer) {
        byteSize = calcPageSize(-byteBuffer.toAvroBytesSize(), -1)
        data.getRecords().removeAt(index)
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
