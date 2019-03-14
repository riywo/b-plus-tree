package com.riywo.ninja.bptree

import org.apache.avro.generic.GenericRecord
import java.nio.ByteBuffer

data class InternalNode(private val page: Page) : Page by page {
    companion object {
        fun new(table: Table, id: Int): InternalNode =
            InternalNode(AvroPage.new(table.key, table.internal, id))
        fun load(table: Table, byteBuffer: ByteBuffer): InternalNode =
            InternalNode(AvroPage.load(table.key, table.internal, byteBuffer))
    }

    private fun findChildPageIdFor(key: GenericRecord): Int? {
        return get(key)?.get(INTERNAL_ID_FIELD_NAME) as? Int
    }
}
