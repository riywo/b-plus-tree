package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*
import java.nio.ByteBuffer

class TestPageManager {
    private val record = ByteBuffer.allocate(MAX_PAGE_SIZE/3)
    private var pageManager = PageManager()
    private var page = pageManager.create(NodeType.RootNode, mutableListOf(record, record, record))

    @BeforeEach
    fun init() {
        pageManager = PageManager()
        page = pageManager.create(NodeType.RootNode, mutableListOf(record, record, record))
        assertThat(pageManager.get(1)).isEqualTo(page)
        assertThat(page.records.size).isEqualTo(3)
    }

    @Test
    fun `split`() {
        val newPage = pageManager.split(page)
        assertThat(page.records.size).isEqualTo(1)
        assertThat(newPage.records.size).isEqualTo(2)
    }
}