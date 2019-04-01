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

import org.junit.jupiter.api.*
import org.junit.jupiter.api.io.*
import org.assertj.core.api.Assertions.*
import org.assertj.core.data.Offset
import java.nio.ByteBuffer
import java.io.File
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
    fun init(@TempDir tempDir: File) {
        val file = tempDir.resolve("test.db")
        val fileManager = FileManager.new(file)
        pageManager = PageManager(fileManager)
        page = pageManager!!.allocate(NodeType.RootNode, MutableList(numRecords){keyValue})
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