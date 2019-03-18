package com.riywo.ninja.bptree

import java.lang.Exception

class Tree(private val table: Table, private val pageManager: PageManager, rootPage: Page) {
    private val rootNode: RootNode = RootNode(table, rootPage)

    fun get(key: Table.Key): Table.Record? {
        val searcher = Searcher()
        val leafNode = searcher.findLeafNode(key, rootNode)
        return leafNode.get(key)
    }

    fun put(record: Table.Record) {
        val searcher = Searcher()
        val leafNode = searcher.findLeafNode(record, rootNode)
        try {
            leafNode.put(record)
        } catch (e: PageFullException) {
            split(leafNode, searcher.internalNodes, record)
        }
    }

    private fun split(leafNode: LeafNode, internalNodes: List<InternalNode>, newRecord: Table.Record) {
        if (internalNodes.isEmpty()) {

        }
    }

    private fun splitRootNode() {

    }

    fun delete(key: Table.Key) {
        val searcher = Searcher()
        val leafNode = searcher.findLeafNode(key, rootNode)
        leafNode.delete(key) // TODO merge check
    }

    private inner class Searcher {
        val internalNodes: MutableList<InternalNode> = mutableListOf()

        fun findLeafNode(key: AvroGenericRecord, internalNode: InternalNode): LeafNode {
            if (internalNode.isLeafNode()) return internalNode // No root yet
            internalNodes.add(internalNode)
            val childPageId = internalNode.findChildPageId(key)
            val childPage = pageManager.get(childPageId) ?: throw Exception() // TODO
            return when (childPage.nodeType) {
                NodeType.LeafNode -> LeafNode(table, childPage)
                NodeType.InternalNode -> findLeafNode(key, InternalNode(table, childPage))
                else -> throw Exception() // TODO
            }
        }
    }
}
