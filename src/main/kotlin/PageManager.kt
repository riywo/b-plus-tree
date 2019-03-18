package com.riywo.ninja.bptree

import NodeType
import java.nio.ByteBuffer

class PageManager {
    private val pool = hashMapOf<Int, Page>()

    fun get(id: Int?): Page? {
        return pool[id]
    }

    fun put(page: Page) {
        pool[page.id] = page
    }

    fun create(nodeType: NodeType, initialRecords: MutableList<ByteBuffer>): Page {
        val maxId = pool.keys.max() ?: 0
        val page = Page.new(maxId + 1, nodeType, initialRecords)
        put(page)
        return page
    }

    fun split(page: Page): Page {
        fun findSplitPoint(it: Iterator<IndexedValue<ByteBuffer>>, accumulatedSize: Int = 0): Int {
            val next = it.next()
            val newSize = accumulatedSize + next.value.limit()
            return when {
                newSize > MAX_PAGE_SIZE/2 -> next.index
                else -> findSplitPoint(it, newSize)
            }
        }
        val splitPoint = findSplitPoint(page.records.withIndex().iterator())
        val movingIndexes = splitPoint until page.records.size
        val newRecords = movingIndexes.map { page.delete(it) }.toMutableList()
        val newPage = create(page.nodeType, newRecords)
        val nextPage = get(page.nextId)
        connect(page, newPage)
        connect(newPage, nextPage)
        return newPage
    }

    private fun connect(previous: Page?, next: Page?) {
        previous?.nextId = next?.id
        next?.previousId = previous?.id
    }
}