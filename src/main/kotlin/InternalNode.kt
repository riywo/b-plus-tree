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
