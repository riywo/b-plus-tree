package com.riywo.ninja.bptree

import org.apache.avro.io.BinaryData
import java.nio.ByteBuffer

fun ByteBuffer.toByteArray(startPosition: Int = 0): ByteArray {
    position(startPosition)
    val bytes = ByteArray(remaining())
    get(bytes)
    rewind()
    return bytes
}

fun ByteArray.toByteBuffer(): ByteBuffer = ByteBuffer.wrap(this)

fun ByteArray.toHexString() = joinToString(":") { String.format("%02x", it) }
fun ByteBuffer.toHexString() = toByteArray().toHexString()

fun ByteBuffer.toAvroBytesSize(): Int {
    val size = limit()
    return size + size.toAvroBytesSize()
}

fun Int.toAvroBytesSize(): Int {
    val bytes = ByteArray(5)
    return BinaryData.encodeInt(this, bytes, 0)
}

fun Int.toLengthAvroByteSize(): Int {
    return when (this) {
        0 -> AVRO_BYTE_SIZE_0
        else -> AVRO_BYTE_SIZE_0 + toAvroBytesSize()
    }
}
