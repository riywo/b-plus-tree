package com.riywo.ninja.bptree

import PageId
import java.nio.ByteBuffer

fun PageId.getInt(): Int? {
    val value = bytes().toByteBuffer().int
    return if (value == AVRO_PAGE_ID_NULL_VALUE) null else value
}

fun PageId.putInt(value: Int?) {
    val byteBuffer = ByteBuffer.allocate(AVRO_PAGE_ID_BYTES)
    byteBuffer.putInt(value ?: AVRO_PAGE_ID_NULL_VALUE)
    bytes(byteBuffer.toByteArray())
}

fun createPageId(id: Int? = null): PageId {
    val pageId = PageId()
    pageId.putInt(id)
    return pageId
}
