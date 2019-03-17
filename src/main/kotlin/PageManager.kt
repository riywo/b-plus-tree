package com.riywo.ninja.bptree

import NodeType

class PageManager {
    private val pool = hashMapOf<Int, Page>()

    fun get(id: Int): Page? {
        return pool[id]
    }

    fun put(page: Page) {
        pool[page.id] = page
    }

    fun create(nodeType: NodeType): Page {
        val maxId = pool.keys.max() ?: 0
        val page = Page.new(maxId + 1, nodeType)
        put(page)
        return page
    }
}