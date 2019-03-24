package com.riywo.ninja.bptree

import KeyValue
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

    fun addChildNode(node: Node) {
        val minKey = node.minRecord.key
        val internal = KeyValue(minKey, encodeChildPageId(node.id))
        val result = find(minKey)
        when (result) {
            is FindResult.FirstGraterThanMatch -> page.insert(result.index, internal)
            null -> page.insert(0, internal)
            else -> throw Exception() // TODO
        }
    }
}
