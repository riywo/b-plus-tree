package com.riywo.ninja.bptree

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
