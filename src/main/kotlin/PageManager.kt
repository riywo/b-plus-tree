package com.riywo.ninja.bptree

import NodeType
import KeyValue

class PageManager(private val fileManager: FileManager) {
    private val pool = hashMapOf<Int, Page>()

    fun get(id: Int?): Page? {
        if (id == null) return null
        return if (pool.contains(id)) {
            pool[id]
        } else {
            val page = fileManager.read(id)
            pool[id] = page
            page
        }
    }

    fun create(nodeType: NodeType, initialRecords: MutableList<KeyValue>): Page {
        return createPage(getMaxId() + 1, nodeType, initialRecords)
    }

    fun split(page: Page): Page {
        fun findSplitPoint(it: Iterator<IndexedValue<KeyValue>>, accumulatedSize: Int = 0): Int? {
            return if (it.hasNext()) {
                val next = it.next()
                val newSize = accumulatedSize + next.value.toAvroBytesSize()
                if (Page.sizeOverhead*2 + newSize*2 >= MAX_PAGE_SIZE) next.index else findSplitPoint(it, newSize)
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
        commit(page)
        commit(newPage)
        commit(nextPage)
        return newPage
    }

    fun move(page: Page, nodeType: NodeType, range: IntRange): Page {
        val records = range.map { page.delete(range.first) }.toMutableList()
        return create(nodeType, records)
    }

    fun commit(page: Page?) {
        if (page != null) {
            fileManager.write(page)
        }
    }

    fun getRootPage(): Page {
        return get(ROOT_PAGE_ID) ?: throw Exception() // TODO
    }

    private fun createPage(id: Int, nodeType: NodeType, initialRecords: MutableList<KeyValue>): Page {
        val page = Page.new(id, nodeType, initialRecords)
        pool[page.id] = page
        return page
    }

    private fun getMaxId(): Int {
        return pool.keys.max() ?: 0 // TODO
    }

    private fun connect(previous: Page?, next: Page?) {
        previous?.nextId = next?.id
        next?.previousId = previous?.id
    }
}