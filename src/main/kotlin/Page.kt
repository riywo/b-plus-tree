package com.riywo.ninja.bptree

import org.apache.avro.generic.GenericRecord
import java.nio.ByteBuffer

interface Page {
    val id: Int
    val size: Int
    val recordsSize: Int
    var sentinelId: Int?
    var previousId: Int?
    var nextId: Int?

    fun dump(): ByteBuffer
    fun records(): List<GenericRecord>
    fun get(key: GenericRecord): GenericRecord?
    fun put(record: GenericRecord)
    fun delete(key: GenericRecord)
}
