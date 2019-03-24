package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*
import org.assertj.core.data.Offset
import java.nio.ByteBuffer

import KeyValue

class TestPageManager {
    private val keyValue = KeyValue(
        ByteBuffer.allocate(1),
        ByteBuffer.allocate(MAX_PAGE_SIZE/100)
    )
    private var pageManager = PageManager()
    private var page = pageManager.create(NodeType.RootNode, MutableList(100){keyValue})

    @BeforeEach
    fun init() {
        pageManager = PageManager()
        page = pageManager.create(NodeType.RootNode, MutableList(100){keyValue})
        assertThat(pageManager.get(1)).isEqualTo(page)
        assertThat(page.records.size).isEqualTo(100)
    }

    @Test
    fun `split at middle`() {
        val newPage = pageManager.split(page)
        assertThat(page.records.size).isCloseTo(50, Offset.offset(1))
        assertThat(newPage.records.size).isCloseTo(50, Offset.offset(1))
    }
}