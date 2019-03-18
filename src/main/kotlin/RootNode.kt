package com.riywo.ninja.bptree

class RootNode(table: Table, page: Page) : InternalNode(table, page) {
    fun splitRoot(pageManager: PageManager): Pair<Page, Page> {
        val newType = if (type == NodeType.RootNode) NodeType.InternalNode else NodeType.LeafNode
        val leftPage = pageManager.move(page, newType, 0 until page.records.size)
        val rightPage = pageManager.split(leftPage)
        return Pair(leftPage, rightPage)
    }
}