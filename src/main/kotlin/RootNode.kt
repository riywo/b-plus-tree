package com.riywo.ninja.bptree

class RootNode(page: Page, compare: KeyCompare) : InternalNode(page, compare) {
    fun splitRoot(pageManager: PageManager): Pair<Page, Page> {
        val newType = if (type == NodeType.RootNode) NodeType.InternalNode else NodeType.LeafNode
        val leftPage = pageManager.move(page, newType, 0 until recordsSize)
        val rightPage = pageManager.split(leftPage)
        return Pair(leftPage, rightPage)
    }
}