package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.junit.jupiter.api.io.*
import org.assertj.core.api.Assertions.*
import org.assertj.core.data.Offset
import java.nio.ByteBuffer
import java.nio.file.Path

class TestFileManager {
    private val page0 = Page.new(0, NodeType.LeafNode, mutableListOf())
    private val page1 = Page.new(1, NodeType.LeafNode, mutableListOf())
    private var fileManager: FileManager? = null

    @BeforeEach
    fun init(@TempDir tempDir: Path) {
        val file = tempDir.resolve("test.db")
        fileManager = FileManager(file.toString())
        fileManager!!.write(page0)
        assertThat(fileManager!!.fileSize.toInt()).isEqualTo(MAX_PAGE_SIZE)
        fileManager!!.write(page1)
        assertThat(fileManager!!.fileSize.toInt()).isEqualTo(MAX_PAGE_SIZE*2)
    }

    @Test
    fun read() {
        val readPage0 = fileManager!!.read(0)
        val readPage1 = fileManager!!.read(1)
        assertThat(readPage0.id).isEqualTo(page0.id)
        assertThat(readPage1.id).isEqualTo(page1.id)
    }
}