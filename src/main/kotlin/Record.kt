package com.riywo.ninja.bptree

import java.nio.ByteBuffer
import KeyValue

data class Record(val key: ByteBuffer, val value: ByteBuffer) {
    constructor(keyValue: KeyValue) : this(keyValue.getKey(), keyValue.getValue())
    constructor(key: ByteArray, value: ByteArray) : this(key.toByteBuffer(), value.toByteBuffer())
    constructor(key: ByteBuffer, value: ByteArray) : this(key, value.toByteBuffer())
    constructor(key: ByteArray, value: ByteBuffer) : this(key.toByteBuffer(), value)
}
