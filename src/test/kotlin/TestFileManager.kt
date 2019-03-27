package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.junit.jupiter.api.io.*
import org.assertj.core.api.Assertions.*
import java.nio.file.Path

class TestFileManager {
    private val page1 = Page.new(1, NodeType.LeafNode, mutableListOf())
    private val page100 = Page.new(100, NodeType.LeafNode, mutableListOf())
    private var fileManager: FileManager? = null

    @BeforeEach
    fun init(@TempDir tempDir: Path) {
        val file = tempDir.resolve("test.db")
        fileManager = FileManager.new(file.toString())
        fileManager!!.write(page1)
        assertThat(fileManager!!.fileSize.toInt()).isEqualTo(MAX_PAGE_SIZE*2 + emptyFileMetadata.toByteBuffer().limit())
        fileManager!!.write(page100)
        assertThat(fileManager!!.fileSize.toInt()).isEqualTo(MAX_PAGE_SIZE*101 + emptyFileMetadata.toByteBuffer().limit())
    }

    @Test
    fun read() {
        val rootPage = fileManager!!.read(ROOT_PAGE_ID)!!
        val readPage1 = fileManager!!.read(1)!!
        val readPage100 = fileManager!!.read(100)!!
        assertThat(rootPage.id).isEqualTo(ROOT_PAGE_ID)
        assertThat(readPage1.id).isEqualTo(page1.id)
        assertThat(readPage100.id).isEqualTo(page100.id)
    }
}