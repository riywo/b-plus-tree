package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*
import org.assertj.core.data.Offset
import java.nio.ByteBuffer

class TestPageManager {
    private val record = ByteBuffer.allocate(MAX_PAGE_SIZE/100)
    private var pageManager = PageManager()
    private var page = pageManager.create(NodeType.RootNode, MutableList(100){record})

    @BeforeEach
    fun init() {
        pageManager = PageManager()
        page = pageManager.create(NodeType.RootNode, MutableList(100){record})
        assertThat(pageManager.get(1)).isEqualTo(page)
        assertThat(page.records.size).isEqualTo(100)
    }

    @Test
    fun `split`() {
        val newPage = pageManager.split(page)
        assertThat(page.records.size).isCloseTo(50, Offset.offset(1))
        assertThat(newPage.records.size).isCloseTo(50, Offset.offset(1))
    }
}