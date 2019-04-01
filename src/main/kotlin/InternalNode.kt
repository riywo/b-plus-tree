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

import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import java.lang.Exception
import java.nio.ByteBuffer

open class InternalNode(page: Page, compare: KeyCompare) : LeafNode(page, compare) {
    companion object {
        private val encoder = EncoderFactory.get().binaryEncoder(ByteArrayOutputStream(), null)
        private val decoder = DecoderFactory.get().binaryDecoder(byteArrayOf(), null)

        fun encodeChildPageId(id: Int): ByteBuffer {
            val output = ByteArrayOutputStream()
            val encoder = EncoderFactory.get().binaryEncoder(output, encoder)
            encoder.writeInt(id)
            encoder.flush()
            return output.toByteArray().toByteBuffer()
        }

        fun decodeChildPageId(byteBuffer: ByteBuffer): Int {
            val decoder = DecoderFactory.get().binaryDecoder(byteBuffer.toByteArray(), decoder)
            return decoder.readInt()
        }
    }

    fun findChildPageId(key: ByteBuffer): Int {
        val result = find(key)
        return when (result) {
            is FindResult.ExactMatch -> decodeChildPageId(result.value)
            is FindResult.FirstGraterThanMatch -> {
                if (result.index == 0 && previousId != null)
                    throw Exception() // TODO
                val index = if (result.index == 0) 0 else result.index-1
                decodeChildPageId(page.records[index].getValue())
            }
            null -> throw Exception() // TODO
        }
    }

    fun firstChildPageId(): Int {
        return decodeChildPageId(records.first().value)
    }

    fun lastChildPageId(): Int {
        return decodeChildPageId(records.last().value)
    }

    fun addChildNode(node: Node, minKey: ByteBuffer = node.minRecord.key) {
        val childPageId = encodeChildPageId(node.id)
        put(minKey, childPageId)
    }
}
