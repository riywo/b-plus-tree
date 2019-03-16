package com.riywo.ninja.bptree

class InternalNode(table: Table, page: AvroPage) : Node(table.key, table.internal, page) {
    fun findChildPageId(key: AvroGenericRecord): Int {
        val result = find(key)
        return when (result) {
            is FindResult.ExactMatch -> createRecord(result.byteBuffer).get(INTERNAL_ID_FIELD_NAME)
            is FindResult.LeftMatch -> createRecord(result.byteBuffer).get(INTERNAL_ID_FIELD_NAME)
            is FindResult.RightMatch -> page.sentinelId
        } as Int
    }

    fun initialize(leftNode: Node, rightNode: Node) {
        if (page.records.isNotEmpty()) throw NodeAlreadyInitializedException("")
        if (leftNode.records.isEmpty()) throw NodeNotInitializedException("")
        if (rightNode.records.isEmpty()) throw NodeNotInitializedException("")
        if (compareKeys(leftNode.records.last(), rightNode.records.first()) != -1) {
            throw InternalNodeInitializeException("")
        }
        page.sentinelId = rightNode.id
        val internal = createRecord(leftNode.records.last())
        internal.put(INTERNAL_ID_FIELD_NAME, leftNode.id)
        page.insert(0, internal.toByteBuffer())
    }
}
