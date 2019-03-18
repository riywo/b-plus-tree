package com.riywo.ninja.bptree

import NodeType
import java.nio.ByteBuffer

abstract class Node(
    protected val table: Table,
    protected val page: Page
) {
    val id get() = page.id
    var type
        get() = page.nodeType
        set(value) { page.nodeType = value }
    var previousId
        get() = page.previousId
        set(value) { page.previousId = value }
    var nextId
        get() = page.nextId
        set(value) { page.nextId = value }
    val size get() = page.size
    val records get() = page.records
    val minRecord get() = page.records.first()

    fun dump() = page.dump()

    fun isLeafNode(): Boolean = type == NodeType.LeafNode
    fun isInternalNode(): Boolean = type == NodeType.InternalNode
    fun isRootNode(): Boolean = type == NodeType.RootNode

    fun get(key: Table.Key): Table.Record? {
        val result = find(key)
        return when (result) {
            is FindResult.ExactMatch -> table.createRecord(result.byteBuffer)
            else -> null
        }
    }

    fun put(record: Table.Record) {
        val byteBuffer = record.toByteBuffer()
        val result = find(record)
        when(result) {
            is FindResult.ExactMatch -> page.update(result.index, byteBuffer) // TODO merge new and old
            is FindResult.FirstGraterThanMatch -> page.insert(result.index, byteBuffer)
            null -> page.insert(0, byteBuffer)
        }
    }

    fun delete(key: Table.Key) {
        val result = find(key)
        if (result is FindResult.ExactMatch) page.delete(result.index)
    }

    fun split(pageManager: PageManager): Page {
        return pageManager.split(page)
    }

    fun printNode(pageManager: PageManager, indent: Int = 0) {
        when (type) {
            NodeType.LeafNode -> {
                println("${"    ".repeat(indent)}$type(id:$id size=$size keys=${records.map{table.createKey(it)}})")
            }
            NodeType.InternalNode, NodeType.RootNode -> {
                println("${"    ".repeat(indent)}$type(id:$id size=$size records=${records.size})")
                records.forEach {
                    val internal = table.createInternal(it)
                    val childPage = pageManager.get(internal.childPageId)!!
                    val child = when(childPage.nodeType) {
                        NodeType.InternalNode -> InternalNode(table, childPage)
                        NodeType.LeafNode -> LeafNode(table, childPage)
                        else -> throw Exception()
                    }
                    println("${"    ".repeat(indent+1)}${table.createKey(it)}")
                    child.printNode(pageManager, indent+1)
                }
            }
        }
    }

    protected sealed class FindResult {
        data class ExactMatch(val index: Int, val byteBuffer: ByteBuffer) : FindResult()
        data class FirstGraterThanMatch(val index: Int) : FindResult()
    }

    protected fun find(key: AvroGenericRecord): FindResult? {
        if (page.records.isEmpty()) return null
        val keyBytes = table.key.encode(key).toByteArray()
        page.records.forEachIndexed { index, byteBuffer ->
            val bytes = byteBuffer.toByteArray()
            when(table.key.compare(bytes, keyBytes)) {
                0 -> return FindResult.ExactMatch(index, byteBuffer)
                1 -> return FindResult.FirstGraterThanMatch(index)
            }
        }
        return FindResult.FirstGraterThanMatch(page.records.size)
    }
}