package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.junit.jupiter.api.io.*
import org.assertj.core.api.Assertions.*
import java.nio.ByteBuffer
import java.nio.file.Path

class TestTree {
    private val compare: KeyCompare = { a, b ->
        a[0].compareTo(b[0])
    }
    private var filePath: String? = null
    private var pageManager: PageManager? = null
    private var tree: Tree? = null
    private val record = Record(byteArrayOf(1), byteArrayOf(1))

    @BeforeEach
    fun init(@TempDir tempDir: Path) {
        filePath = tempDir.resolve("test.db").toString()
        pageManager = PageManager(FileManager.new(filePath!!))
        tree = Tree(pageManager!!, compare)
        tree!!.put(record)
    }

    @Test
    fun `get root-only`() {
        assertThat(tree!!.get(record.key)).isEqualTo(record)
    }

    @Test
    fun `update root-only`() {
        val newRecord = Record(record.key, byteArrayOf(2))
        tree!!.put(newRecord)
        assertThat(tree!!.get(record.key)).isEqualTo(newRecord)
    }

    @Test
    fun `delete root-only`() {
        tree!!.delete(record)
        assertThat(tree!!.get(record.key)).isEqualTo(null)
    }

    @Test
    fun `load root-only`() {
        val loadedPageManager = PageManager(FileManager.load(filePath!!))
        val loadedTree = Tree(loadedPageManager, compare)
        assertThat(loadedTree.get(record.key)).isEqualTo(record)
    }

    @Test
    fun `insert ordered`() {
        val records = (2..120).map { Record(byteArrayOf(it.toByte()), ByteBuffer.allocate(MAX_PAGE_SIZE/5)) }
        for (newRecord in records) {
            tree!!.put(newRecord)
        }
        val scanned = tree!!.scan(records.first().key, records.last().key).toList()
        assertThat(scanned).isEqualTo(records)
        val scannedReversed = tree!!.scan(records.last().key, records.first().key).toList()
        assertThat(scannedReversed).isEqualTo(records.reversed())
        //tree!!.debug()
    }

    @Test
    fun `insert reversed ordered`() {
        val records = (2..120).map { Record(byteArrayOf(it.toByte()), ByteBuffer.allocate(MAX_PAGE_SIZE/5)) }
        for (newRecord in records.reversed()) {
            tree!!.put(newRecord)
        }
        val scanned = tree!!.scan(records.first().key, records.last().key).toList()
        assertThat(scanned).isEqualTo(records)
        val scannedReversed = tree!!.scan(records.last().key, records.first().key).toList()
        assertThat(scannedReversed).isEqualTo(records.reversed())
        //tree!!.debug()
    }

    @Test
    fun `insert shuffled ordered`() {
        val records = (2..120).map { Record(byteArrayOf(it.toByte()), ByteBuffer.allocate(MAX_PAGE_SIZE/5)) }
        for (newRecord in records.shuffled()) {
            tree!!.put(newRecord)
        }
        val scanned = tree!!.scan(records.first().key, records.last().key).toList()
        assertThat(scanned).isEqualTo(records)
        val scannedReversed = tree!!.scan(records.last().key, records.first().key).toList()
        assertThat(scannedReversed).isEqualTo(records.reversed())
        //tree!!.debug()
    }
}