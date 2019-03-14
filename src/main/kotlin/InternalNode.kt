package com.riywo.ninja.bptree

import java.nio.ByteBuffer

data class InternalNode(private val page: Page) : Page by page {
    companion object {
        fun new(table: Table, id: Int): InternalNode =
            InternalNode(AvroPage.new(table.key, table.internal, id))
        fun load(table: Table, byteBuffer: ByteBuffer): InternalNode =
            InternalNode(AvroPage.load(table.key, table.internal, byteBuffer))
    }

//    private fun findChildPageIdFor(key: GenericRecord): Int? {
//
//    }
}
