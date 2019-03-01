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
import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*
import java.lang.IndexOutOfBoundsException
import java.nio.ByteBuffer

class TestPage {
    private var page = Page.new(1, NodeType.RootNode, mutableListOf())
    private val keyValue = KeyValue(ByteBuffer.allocate(10), ByteBuffer.allocate(10))

    @BeforeEach
    fun init() {
        page = Page.new(1, NodeType.RootNode, mutableListOf())
        page.insert(0, keyValue)
        assertThat(page.id).isEqualTo(1)
        assertThat(page.nodeType).isEqualTo(NodeType.RootNode)
        assertThat(page.previousId).isEqualTo(null)
        assertThat(page.nextId).isEqualTo(null)
        assertThat(page.records.size).isEqualTo(1)
        assertThat(page.records).isEqualTo(listOf(keyValue))
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `dump and load`() {
        val pageLoaded = Page.load(page.dump())
        assertThat(pageLoaded.id).isEqualTo(page.id)
        assertThat(pageLoaded.dump()).isEqualTo(page.dump())
    }

    @Test
    fun `insert byteBuffer`() {
        val newKeyValue = KeyValue(ByteBuffer.allocate(9), ByteBuffer.allocate(9))
        page.insert(0, newKeyValue)
        page.insert(2, newKeyValue)
        assertThat(page.records).isEqualTo(listOf(newKeyValue, keyValue, newKeyValue))
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `update byteBuffer`() {
        val newKeyValue = KeyValue(ByteBuffer.allocate(9), ByteBuffer.allocate(9))
        page.update(0, newKeyValue)
        assertThat(page.records).isEqualTo(listOf(newKeyValue))
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `delete byteBuffer`() {
        page.delete(0)
        assertThat(page.records).isEqualTo(listOf<KeyValue>())
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `insert full page`() {
        val newKeyValue = KeyValue(ByteBuffer.allocate(9), ByteBuffer.allocate(MAX_PAGE_SIZE))
        assertThrows<PageFullException> {
            page.insert(1, newKeyValue)
        }
        assertThat(page.records).isEqualTo(listOf(keyValue, newKeyValue))
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `update full page`() {
        val newKeyValue = KeyValue(ByteBuffer.allocate(10), ByteBuffer.allocate(MAX_PAGE_SIZE))
        assertThrows<PageFullException> {
            page.update(0, newKeyValue)
        }
        assertThat(page.records).isEqualTo(listOf(newKeyValue))
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `can't insert wrong index`() {
        for (i in listOf(-1, 2, 100)) {
            assertThrows<IndexOutOfBoundsException> { page.insert(i, keyValue) }
        }
        assertThat(page.records).isEqualTo(listOf(keyValue))
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `can't insert 0 if previousId exists`() {
        page.previousId = 2
        assertThrows<PageInsertingMinimumException> { page.insert(0, keyValue) }
        assertThat(page.records).isEqualTo(listOf(keyValue))
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `can't update wrong index`() {
        for (i in listOf(-1, 1, 100)) {
            assertThrows<IndexOutOfBoundsException> { page.update(i, keyValue) }
        }
        assertThat(page.records).isEqualTo(listOf(keyValue))
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `can't delete wrong index`() {
        for (i in listOf(-1, 1, 100)) {
            assertThrows<IndexOutOfBoundsException> { page.delete(i) }
        }
        assertThat(page.records).isEqualTo(listOf(keyValue))
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `manipulate previousId`() {
        page.previousId = 1
        assertThat(page.previousId!!).isEqualTo(1)
        assertThat(page.size).isEqualTo(page.dump().limit())

        val pageLoaded = Page.load(page.dump())
        assertThat(pageLoaded.previousId).isEqualTo(1)
        assertThat(pageLoaded.dump()).isEqualTo(page.dump())

        page.previousId = null
        val newSentinelId = page.previousId
        assertThat(newSentinelId).isEqualTo(null)
        assertThat(page.size).isEqualTo(page.dump().limit())

        val pageLoaded2 = Page.load(page.dump())
        assertThat(pageLoaded2.previousId).isEqualTo(null)
        assertThat(pageLoaded2.dump()).isEqualTo(page.dump())
    }

    @Test
    fun `manipulate nodeType`() {
        page.nodeType = NodeType.LeafNode
        assertThat(page.nodeType).isEqualTo(NodeType.LeafNode)
        assertThat(page.size).isEqualTo(page.dump().limit())

        val pageLoaded = Page.load(page.dump())
        assertThat(pageLoaded.nodeType).isEqualTo(NodeType.LeafNode)
        assertThat(pageLoaded.dump()).isEqualTo(page.dump())
    }
}