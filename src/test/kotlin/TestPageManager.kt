package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.junit.jupiter.api.io.*
import org.assertj.core.api.Assertions.*
import org.assertj.core.data.Offset
import java.nio.ByteBuffer
import java.nio.file.Path
import KeyValue

class TestPageManager {
    private val numRecords = 20
    private val keyValue = KeyValue(
        ByteBuffer.allocate(1),
        ByteBuffer.allocate(MAX_PAGE_SIZE/numRecords - 4)
    )
    private var pageManager: PageManager? = null
    private var page: Page? = null

    @BeforeEach
    fun init(@TempDir tempDir: Path) {
        pageManager = PageManager(tempDir.resolve("test.db").toString())
        page = pageManager!!.create(NodeType.RootNode, MutableList(numRecords){keyValue})
        assertThat(pageManager!!.get(1)).isEqualTo(page)
        assertThat(page!!.records.size).isEqualTo(numRecords)
    }

    @Test
    fun `split at middle`() {
        val newPage = pageManager!!.split(page!!)
        assertThat(page!!.records.size).isCloseTo(numRecords/2, Offset.offset(1))
        assertThat(newPage.records.size).isCloseTo(numRecords/2, Offset.offset(1))
    }
}