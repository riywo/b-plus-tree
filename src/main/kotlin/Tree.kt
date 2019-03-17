package com.riywo.ninja.bptree

class Tree(private val table: Table, private val pageManager: PageManager) {
    private val rootNode: RootNode = RootNode(table, pageManager.create(NodeType.RootNode))

    fun get(key: Table.Key): Table.Record? {
        return findLeafNode(key, rootNode)?.get(key) as Table.Record
    }

    fun put(record: Table.Record) {
        findLeafNode(record, rootNode)?.put(record)
    }

    fun delete(key: Table.Key) {
        findLeafNode(key, rootNode)?.delete(key)
    }

    private fun findLeafNode(key: AvroGenericRecord, internalNode: InternalNode): LeafNode? {
        if (internalNode.type == NodeType.LeafNode) return internalNode // No root node
        val childPageId = internalNode.findChildPageId(key)
        val childPage = pageManager.get(childPageId!!)
        return when (childPage?.nodeType) {
            NodeType.LeafNode -> LeafNode(table, childPage)
            NodeType.InternalNode -> findLeafNode(key, InternalNode(table, childPage))
            else -> null
        }
    }
}