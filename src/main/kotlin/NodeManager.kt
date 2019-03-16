package com.riywo.ninja.bptree

import NodeType

class NodeManager(private val table: Table) {
    class ReturnNode(val internal: InternalNode? = null, val leaf: LeafNode? = null)

    private val pool = hashMapOf<Int, Node>()

    fun get(id: Int): ReturnNode? {
        val node = pool[id] ?: return null
        return when (node.type) {
            NodeType.InternalNode -> ReturnNode(internal = node as InternalNode)
            NodeType.LeafNode -> ReturnNode(leaf = node as LeafNode)
        }
    }

    fun put(node: Node) {
        pool[node.id] = node
    }

    fun create(nodeType: NodeType): ReturnNode? {
        val maxId = pool.keys.max() ?: 0
        val page = Page.new(maxId + 1, nodeType)
        val node = when (nodeType) {
            NodeType.InternalNode -> InternalNode(table, page)
            NodeType.LeafNode -> LeafNode(table, page)
        }
        put(node)
        return get(node.id)
    }
}