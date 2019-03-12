package com.riywo.ninja.bptree

import java.nio.ByteBuffer

interface Page {
    fun id(): Int
    fun size(): Int
    fun records(): List<ByteBuffer>
    fun dump(): ByteBuffer
    fun get(keyByteBuffer: ByteBuffer): ByteBuffer?
    fun put(keyByteBuffer: ByteBuffer, recordByteBuffer: ByteBuffer)
    fun delete(keyByteBuffer: ByteBuffer)
}
