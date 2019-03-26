package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.junit.jupiter.api.io.*
import org.assertj.core.api.Assertions.*
import java.nio.file.Path

class TestFileManager {
    private val page0 = Page.new(0, NodeType.LeafNode, mutableListOf())
    private val page100 = Page.new(100, NodeType.LeafNode, mutableListOf())
    private var fileManager: FileManager? = null

    @BeforeEach
    fun init(@TempDir tempDir: Path) {
        val file = tempDir.resolve("test.db")
        fileManager = FileManager(file.toString())
        fileManager!!.write(page0)
        assertThat(fileManager!!.fileSize.toInt()).isEqualTo(MAX_PAGE_SIZE)
        fileManager!!.write(page100)
        assertThat(fileManager!!.fileSize.toInt()).isEqualTo(MAX_PAGE_SIZE*101)
    }

    @Test
    fun read() {
        val readPage0 = fileManager!!.read(0)
        val readPage100 = fileManager!!.read(100)
        assertThat(readPage0.id).isEqualTo(page0.id)
        assertThat(readPage100.id).isEqualTo(page100.id)
    }
}