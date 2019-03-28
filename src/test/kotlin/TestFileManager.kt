package com.riywo.ninja.bptree

import java.io.File
import org.apache.avro.SchemaBuilder
import org.junit.jupiter.api.*
import org.junit.jupiter.api.io.*
import org.assertj.core.api.Assertions.*

class TestFileManager {
    private val page1 = Page.new(1, NodeType.LeafNode, mutableListOf())
    private val page100 = Page.new(100, NodeType.LeafNode, mutableListOf())
    private val keySchema = SchemaBuilder.builder().record("key").fields()
        .name("key").type().intType().noDefault().endRecord()
    private val valueSchema = SchemaBuilder.builder().record("value").fields()
        .name("value").type().stringType().noDefault().endRecord()
    private var fileManager: FileManager? = null

    @BeforeEach
    fun init(@TempDir tempDir: File) {
        val file = tempDir.resolve("test.db")
        fileManager = FileManager.new(file, keySchema, valueSchema)
        fileManager!!.write(page1)
        fileManager!!.write(page100)
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