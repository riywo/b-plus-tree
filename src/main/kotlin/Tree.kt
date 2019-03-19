package com.riywo.ninja.bptree

import java.lang.Exception

class Tree(private val table: Table, private val pageManager: PageManager, rootPage: Page) {
    private val rootNode: RootNode = RootNode(table, rootPage)

    fun get(key: Table.Key): Table.Record? {
        val result = findLeafNode(key)
        return result.leafNode.get(key)
    }

    fun put(record: Table.Record) {
        val result = findLeafNode(record)
        try {
            result.leafNode.put(record)
        } catch (e: PageFullException) {
            splitNode(result.leafNode, result.pathFromRoot.reversed().iterator())
        }
    }

    fun scan(startKey: Table.Key, endKey: Table.Key): Sequence<Table.Record> {
        val isAscending = table.key.compare(startKey, endKey) == -1
        val startByteBuffer = startKey.toByteBuffer()
        val endByteBuffer = endKey.toByteBuffer()
        val firstNode = findLeafNode(startKey).leafNode
        return if (isAscending) {
            generateSequence(firstNode) { createLeafNode(it.nextId) }
                .flatMap { it.records.asSequence() }
                .dropWhile { table.key.compare(it, startByteBuffer) == -1 } // it < startKey
                .takeWhile { table.key.compare(it, endByteBuffer) != 1 }    // it <= endKey
                .map { table.createRecord(it) }
        } else {
            generateSequence(firstNode) { createLeafNode(it.previousId) }
                .flatMap { it.records.reversed().asSequence() }
                .dropWhile { table.key.compare(it, startByteBuffer) == 1 } // it > startKey
                .takeWhile { table.key.compare(it, endByteBuffer) != -1 }  // it >= endKey
                .map { table.createRecord(it) }
        }
    }

    private fun createLeafNode(id: Int?): LeafNode? {
        val page = pageManager.get(id) ?: return null
        return LeafNode(table, page)
    }

    private data class FindResult(val leafNode: LeafNode, val pathFromRoot: List<InternalNode>)

    private fun findLeafNode(
        key: AvroGenericRecord,
        parentNode: InternalNode = rootNode, pathFromRoot: List<InternalNode> = listOf()): FindResult {
        if (parentNode.isLeafNode()) return FindResult(parentNode, pathFromRoot) // No root yet
        val newPathFromRoot = pathFromRoot + parentNode
        val childPageId = parentNode.findChildPageId(key)
        val childPage = pageManager.get(childPageId) ?: throw Exception() // TODO
        return when (childPage.nodeType) {
            NodeType.LeafNode -> FindResult(LeafNode(table, childPage), newPathFromRoot)
            NodeType.InternalNode -> findLeafNode(key, InternalNode(table, childPage), newPathFromRoot)
            else -> throw Exception() // TODO
        }
    }

    private fun splitNode(node: Node, pathToRoot: Iterator<InternalNode>) {
        if (pathToRoot.hasNext()) {
            val parent = pathToRoot.next()
            val newNode = when (node.type) {
                NodeType.LeafNode -> LeafNode(table, node.split(pageManager))
                NodeType.InternalNode -> InternalNode(table, node.split(pageManager))
                else -> throw Exception() // TODO
            }
            try {
                parent.addChildNode(newNode)
            } catch (e: PageFullException) {
                splitNode(parent, pathToRoot)
            }
        } else {
            splitRootNode()
        }
    }

    private fun splitRootNode() {
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

    fun delete(key: Table.Key) {
        val result = findLeafNode(key)
        result.leafNode.delete(key) // TODO merge
    }

    fun debug() {
        rootNode.printNode(pageManager)
    }
}
