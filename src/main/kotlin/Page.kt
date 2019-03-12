package com.riywo.ninja.bptree

import org.apache.avro.generic.GenericRecord
import java.nio.ByteBuffer

interface Page {
    fun id(): Int
    fun size(): Int
    fun dump(): ByteBuffer

    fun records(): List<ByteBuffer>

    fun get(keyByteBuffer: ByteBuffer): ByteBuffer?
    fun get(key: GenericRecord): GenericRecord?

    fun put(keyByteBuffer: ByteBuffer, recordByteBuffer: ByteBuffer)
    fun put(record: GenericRecord)

    fun delete(keyByteBuffer: ByteBuffer)
    fun delete(key: GenericRecord)
}
