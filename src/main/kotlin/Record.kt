package com.riywo.ninja.bptree

import java.nio.ByteBuffer
import KeyValue

data class Record(val key: ByteBuffer, val value: ByteBuffer) {
    constructor(keyValue: KeyValue) : this(keyValue.getKey(), keyValue.getValue())
    constructor(keyBytes: ByteArray, valueBytes: ByteArray)
            : this(keyBytes.toByteBuffer(), valueBytes.toByteBuffer())
    constructor(key: ByteBuffer, valueBytes: ByteArray)
            : this(key, valueBytes.toByteBuffer())
    constructor(keyBytes: ByteArray, value: ByteBuffer)
            : this(keyBytes.toByteBuffer(), value)
}
