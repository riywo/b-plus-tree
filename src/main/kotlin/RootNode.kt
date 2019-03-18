package com.riywo.ninja.bptree

class RootNode(table: Table, page: Page) : InternalNode(table, page) {
    fun splitRoot(pageManager: PageManager): Pair<Page, Page> {
        val leftPage = pageManager.move(page, NodeType.InternalNode, 0 until page.records.size)
        val rightPage = pageManager.split(leftPage)
        return Pair(leftPage, rightPage)
    }
}