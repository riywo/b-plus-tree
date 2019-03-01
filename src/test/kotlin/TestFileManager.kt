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

import java.io.File
import org.junit.jupiter.api.*
import org.junit.jupiter.api.io.*
import org.assertj.core.api.Assertions.*

class TestFileManager {
    private val page1 = Page.new(1, NodeType.LeafNode, mutableListOf())
    private val page100 = Page.new(100, NodeType.LeafNode, mutableListOf())
    private var fileManager: FileManager? = null

    @BeforeEach
    fun init(@TempDir tempDir: File) {
        val file = tempDir.resolve("test.db")
        fileManager = FileManager.new(file)
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