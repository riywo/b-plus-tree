package com.riywo.ninja.bptree

import NodeType
import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*
import java.lang.IndexOutOfBoundsException
import java.nio.ByteBuffer

class TestPage {
    private var page = Page.new(1, NodeType.LeafNode)
    private val byteBuffer = ByteBuffer.allocate(10)

    @BeforeEach
    fun init() {
        page = Page.new(1, NodeType.LeafNode)
        page.insert(0, byteBuffer)
        assertThat(page.id).isEqualTo(1)
        assertThat(page.nodeType).isEqualTo(NodeType.LeafNode)
        assertThat(page.previousId).isEqualTo(null)
        assertThat(page.nextId).isEqualTo(null)
        assertThat(page.records.size).isEqualTo(1)
        assertThat(page.records).isEqualTo(listOf(byteBuffer))
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
        val newByteBuffer = ByteBuffer.allocate(9)
        page.insert(0, newByteBuffer)
        page.insert(2, newByteBuffer)
        assertThat(page.records).isEqualTo(listOf(newByteBuffer, byteBuffer, newByteBuffer))
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `update byteBuffer`() {
        val newByteBuffer = ByteBuffer.allocate(9)
        page.update(0, newByteBuffer)
        assertThat(page.records).isEqualTo(listOf(newByteBuffer))
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `delete byteBuffer`() {
        page.delete(0)
        assertThat(page.records).isEqualTo(listOf<ByteBuffer>())
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `can't insert full page`() {
        val newByteBuffer = ByteBuffer.allocate(MAX_PAGE_SIZE)
        assertThrows<PageFullException> {
            page.insert(1, newByteBuffer)
        }
        assertThat(page.records).isEqualTo(listOf(byteBuffer))
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `can't update full page`() {
        val newByteBuffer = ByteBuffer.allocate(MAX_PAGE_SIZE)
        assertThrows<PageFullException> {
            page.update(0, newByteBuffer)
        }
        assertThat(page.records).isEqualTo(listOf(byteBuffer))
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `can't insert wrong index`() {
        for (i in listOf(-1, 2, 100)) {
            assertThrows<IndexOutOfBoundsException> { page.insert(i, byteBuffer) }
        }
        assertThat(page.records).isEqualTo(listOf(byteBuffer))
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `can't insert 0 if previousId exists`() {
        page.previousId = 2
        assertThrows<PageInsertingMinimumException> { page.insert(0, byteBuffer) }
        assertThat(page.records).isEqualTo(listOf(byteBuffer))
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `can't update wrong index`() {
        for (i in listOf(-1, 1, 100)) {
            assertThrows<IndexOutOfBoundsException> { page.update(i, byteBuffer) }
        }
        assertThat(page.records).isEqualTo(listOf(byteBuffer))
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `can't delete wrong index`() {
        for (i in listOf(-1, 1, 100)) {
            assertThrows<IndexOutOfBoundsException> { page.delete(i) }
        }
        assertThat(page.records).isEqualTo(listOf(byteBuffer))
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
}