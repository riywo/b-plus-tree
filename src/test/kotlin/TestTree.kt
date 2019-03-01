/**
 * Copyright 2019 Ryosuke IWANAGA <me@riywo.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.riywo.ninja.bptree

import org.apache.avro.SchemaBuilder
import org.junit.jupiter.api.*
import org.junit.jupiter.api.io.*
import org.assertj.core.api.Assertions.*
import java.nio.ByteBuffer
import java.io.File

class TestTree {
    private val compare: KeyCompare = { a, b ->
        a.toByteBuffer().short.compareTo(b.toByteBuffer().short)
    }
    private var file: File? = null
    private var pageManager: PageManager? = null
    private var tree: Tree? = null
    private val record = Record(makeKey(1), byteArrayOf(1))

    private fun makeKey(i: Int): ByteArray {
        return ByteBuffer.allocate(2).putShort(i.toShort()).toByteArray()
    }

    @BeforeEach
    fun init(@TempDir tempDir: File) {
        file = tempDir.resolve("test.db")
        val fileManager = FileManager.new(file!!)
        pageManager = PageManager(fileManager)
        tree = Tree(pageManager!!, compare)
    }

    @Test
    fun `get root-only`() {
        tree!!.put(record)
        assertThat(tree!!.get(record.key)).isEqualTo(record)
    }

    @Test
    fun `update root-only`() {
        tree!!.put(record)
        val newRecord = Record(record.key, byteArrayOf(2))
        tree!!.put(newRecord)
        assertThat(tree!!.get(record.key)).isEqualTo(newRecord)
    }

    @Test
    fun `delete root-only`() {
        tree!!.put(record)
        tree!!.delete(record)
        assertThat(tree!!.get(record.key)).isEqualTo(null)
    }

    @Test
    fun `load root-only`() {
        tree!!.put(record)
        val loadedPageManager = PageManager(FileManager.load(file!!))
        val loadedTree = Tree(loadedPageManager, compare)
        assertThat(loadedTree.get(record.key)).isEqualTo(record)
    }

    @Test
    fun `insert ordered`() {
        val records = (1..50).map { Record(makeKey(it), ByteBuffer.allocate(MAX_PAGE_SIZE/6)) }
        for (newRecord in records) {
            tree!!.put(newRecord)
        }
        val scanned = tree!!.scan().toList()
        assertThat(scanned).isEqualTo(records)
        val scannedReversed = tree!!.scan(records.last().key, records.first().key).toList()
        assertThat(scannedReversed).isEqualTo(records.reversed())
        tree!!.debug()

        val loadedPageManager = PageManager(FileManager.load(file!!))
        val loadedTree = Tree(loadedPageManager, compare)
        val loadedScanned = loadedTree.scan().toList()
        assertThat(loadedScanned).isEqualTo(records)
    }

    @Test
    fun `insert reversed ordered`() {
        val records = (1..50).map { Record(makeKey(it), ByteBuffer.allocate(MAX_PAGE_SIZE/6)) }
        for (newRecord in records.reversed()) {
            tree!!.put(newRecord)
        }
        val scanned = tree!!.scan().toList()
        assertThat(scanned).isEqualTo(records)
        val scannedReversed = tree!!.scan(records.last().key, records.first().key).toList()
        assertThat(scannedReversed).isEqualTo(records.reversed())
        tree!!.debug()

        val loadedPageManager = PageManager(FileManager.load(file!!))
        val loadedTree = Tree(loadedPageManager, compare)
        val loadedScanned = loadedTree.scan().toList()
        assertThat(loadedScanned).isEqualTo(records)
    }

    @Test
    fun `insert shuffled ordered`() {
        val records = (1..50).map { Record(makeKey(it), ByteBuffer.allocate(MAX_PAGE_SIZE/6)) }
        for (newRecord in records.shuffled()) {
            tree!!.put(newRecord)
        }
        val scanned = tree!!.scan().toList()
        assertThat(scanned).isEqualTo(records)
        val scannedReversed = tree!!.scan(records.last().key, records.first().key).toList()
        assertThat(scannedReversed).isEqualTo(records.reversed())
        tree!!.debug()

        val loadedPageManager = PageManager(FileManager.load(file!!))
        val loadedTree = Tree(loadedPageManager, compare)
        val loadedScanned = loadedTree.scan().toList()
        assertThat(loadedScanned).isEqualTo(records)
    }
}