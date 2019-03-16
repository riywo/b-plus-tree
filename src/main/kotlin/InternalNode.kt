package com.riywo.ninja.bptree

import org.apache.avro.generic.GenericRecord

class InternalNode(table: Table, page: AvroPage) : Node(table.key, table.internal, page) {
    fun findChildPageId(key: GenericRecord): Int {
        val result = find(key)
        return when (result) {
            is FindResult.ExactMatch -> createRecord(result.byteBuffer).get(INTERNAL_ID_FIELD_NAME)
            is FindResult.LeftMatch -> createRecord(result.byteBuffer).get(INTERNAL_ID_FIELD_NAME)
            is FindResult.RightMatch -> page.sentinelId
        } as Int
    }


}
