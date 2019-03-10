package com.riywo.ninja.bptree

import LeafNodePage
import LeafNodeCell
import org.apache.avro.io.*
import org.apache.avro.specific.*
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

fun ByteBuffer.toByteArray(): ByteArray {
    rewind()
    val bytes = ByteArray(remaining())
    get(bytes)
    rewind()
    return bytes
}

fun ByteArray.toByteBuffer(): ByteBuffer {
    return ByteBuffer.wrap(this)
}

class LeafNode(private val table: Table) {
    private val page = LeafNodePage(mutableListOf<LeafNodeCell>())

    fun load(pageBytes: ByteArray) {
        val reader = SpecificDatumReader<LeafNodePage>(LeafNodePage::class.java)
        val decoder = DecoderFactory.get().binaryDecoder(pageBytes, null)
        reader.read(page, decoder)
    }

    fun dump(): ByteArray {
        val output = ByteArrayOutputStream()
        val writer = SpecificDatumWriter<LeafNodePage>(LeafNodePage::class.java)
        val encoder = EncoderFactory.get().binaryEncoder(output, null)
        writer.write(page, encoder)
        encoder.flush()
        return output.toByteArray()
    }

    val keys: List<ByteBuffer>
        get() = cells().map { it.getKey() }

    fun get(keyBytes: ByteArray): ByteBuffer? {
        val result = findKey(keyBytes)
        return when(result) {
            is FindKeyResult.Found -> result.value
            is FindKeyResult.NotFound -> null
        }
    }

    fun put(keyBytes: ByteArray, valueBytes: ByteArray) {
        val cell = LeafNodeCell()
        cell.setKey(keyBytes.toByteBuffer())
        cell.setValue(valueBytes.toByteBuffer())
        val result = findKey(keyBytes)
        when(result) {
            is FindKeyResult.Found -> update(result.putIndex, cell)
            is FindKeyResult.NotFound -> insert(result.putIndex, cell)
        }
    }

    private fun cells(): MutableList<LeafNodeCell> {
        return page.getCells()
    }

    private sealed class FindKeyResult {
        data class Found(val putIndex: Int, val value: ByteBuffer) : FindKeyResult()
        data class NotFound(val putIndex: Int): FindKeyResult()
    }

    private fun findKey(keyBytes: ByteArray): FindKeyResult {
        val cells = cells()
        cells.forEachIndexed { index, cell ->
            when(table.key.compare(cell.getKey().toByteArray(), keyBytes)) {
                0 -> return FindKeyResult.Found(index, cell.getValue())
                1 -> return FindKeyResult.NotFound(index - 1)
            }
        }
        return FindKeyResult.NotFound(cells.size)
    }

    private fun insert(index: Int, cell: LeafNodeCell) {
        cells().add(index, cell)
    }

    private fun update(index: Int, cell: LeafNodeCell) {
        cells()[index] = cell
    }
}
