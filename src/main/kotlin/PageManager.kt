/**
 * Copyright 2019 Ryosuke IWANAGA <me@riywo.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
            val page = fileManager.read(id) ?: throw java.lang.Exception() // TODO
            pool[id] = page
            page
        }
    }

    fun allocate(nodeType: NodeType, initialRecords: MutableList<KeyValue>): Page {
        val page = fileManager.allocate(nodeType, initialRecords)
        pool[page.id] = page
        return page
    }

    fun split(page: Page): Page {
        fun findSplitPoint(it: Iterator<IndexedValue<KeyValue>>, accumulatedSize: Int = 0): Int? {
            return if (it.hasNext()) {
                val next = it.next()
                val newSize = accumulatedSize + next.value.toAvroBytesSize()
                if (Page.sizeOverhead + newSize > MAX_PAGE_SIZE/2) next.index+1 else findSplitPoint(it, newSize)
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
        return allocate(nodeType, records)
    }

    fun commit(page: Page?) {
        if (page != null) {
            fileManager.write(page)
        }
    }

    fun getRootPage(): Page {
        return get(ROOT_PAGE_ID) ?: throw Exception() // TODO
    }

    private fun connect(previous: Page?, next: Page?) {
        previous?.nextId = next?.id
        next?.previousId = previous?.id
    }
}