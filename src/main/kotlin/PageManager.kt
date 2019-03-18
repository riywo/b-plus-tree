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
        fun findSplitPoint(it: Iterator<IndexedValue<ByteBuffer>>, accumulatedSize: Int = 0): Int? {
            return if (it.hasNext()) {
                val next = it.next()
                val newSize = accumulatedSize + next.value.limit()
                if (Page.sizeOverhead + newSize*2 >= MAX_PAGE_SIZE) next.index else findSplitPoint(it, newSize)
            } else {
                null
            }
        }
        val splitPoint = findSplitPoint(page.records.withIndex().iterator()) ?: page.records.size-1
        val movingIndexes = splitPoint until page.records.size
        val newPage = move(page, page.nodeType, movingIndexes)
        val nextPage = get(page.nextId)
        connect(page, newPage)
        connect(newPage, nextPage)
        return newPage
    }

    fun move(page: Page, nodeType: NodeType, range: IntRange): Page {
        val records = range.map { page.delete(range.first) }.toMutableList()
        return create(nodeType, records)
    }

    private fun connect(previous: Page?, next: Page?) {
        previous?.nextId = next?.id
        next?.previousId = previous?.id
    }
}