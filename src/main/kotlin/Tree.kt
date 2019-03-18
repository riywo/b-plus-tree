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
            splitNode(leafNode, searcher.internalNodes.iterator())
        }
    }

    private fun splitNode(node: Node, it: Iterator<InternalNode>) {
        if (it.hasNext()) {
            val newNode = when (node.type) {
                NodeType.LeafNode -> LeafNode(table, node.split(pageManager))
                NodeType.InternalNode -> InternalNode(table, node.split(pageManager))
                else -> throw Exception() // TODO
            }
            val parent = it.next()
            try {
                parent.addChildNode(newNode)
            } catch (e: PageFullException) {
                splitNode(parent, it)
            }
        } else {
            // root node split
            val (leftPage, rightPage) = rootNode.splitRoot(pageManager)
            val (leftNode, rightNode) = when (rootNode.type) {
                NodeType.LeafNode -> Pair(LeafNode(table, leftPage), LeafNode(table, rightPage))
                NodeType.RootNode -> Pair(InternalNode(table, leftPage), InternalNode(table, rightPage))
                else -> throw Exception() // TODO
            }
            rootNode.addChildNode(leftNode)
            rootNode.addChildNode(rightNode)
            rootNode.type = NodeType.RootNode
        }
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
