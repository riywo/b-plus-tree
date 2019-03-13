package com.riywo.ninja.bptree

import java.nio.ByteBuffer

data class LeafNode(private val page: Page) : Page by page {
    companion object {
        fun new(table: Table, id: Int): LeafNode =
            LeafNode(AvroPage.new(table.key, table.record, id))
        fun load(table: Table, byteBuffer: ByteBuffer): LeafNode =
            LeafNode(AvroPage.load(table.key, table.record, byteBuffer))
    }
}
