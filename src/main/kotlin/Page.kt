package com.riywo.ninja.bptree

import org.apache.avro.generic.GenericRecord
import java.nio.ByteBuffer

interface Page {
    fun id(): Int
    fun size(): Int
    fun dump(): ByteBuffer
    fun records(): List<GenericRecord>
    fun recordsSize(): Int
    fun get(key: GenericRecord): GenericRecord?
    fun put(record: GenericRecord)
    fun delete(key: GenericRecord)
}
