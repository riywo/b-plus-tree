package com.riywo.ninja.bptree

import org.apache.avro.io.BinaryData
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.DecoderFactory
import java.nio.ByteBuffer

// ByteArray

fun ByteArray.toByteBuffer(): ByteBuffer = ByteBuffer.wrap(this)

fun ByteArray.toHexString() = joinToString(":") { String.format("%02x", it) }

// ByteBuffer

fun ByteBuffer.toByteArray(buffer: ByteArray? = null): ByteArray {
    position(0)
    val bytes = buffer ?: ByteArray(remaining())
    get(bytes, 0, remaining())
    rewind()
    return bytes
}

fun ByteBuffer.toAvroBytesSize(): Int {
    val size = limit()
    return size + size.toAvroBytesSize()
}

fun ByteBuffer.toHexString() = toByteArray().toHexString()

// Int

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
