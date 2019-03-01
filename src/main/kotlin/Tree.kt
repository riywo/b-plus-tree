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

import java.lang.Exception
import java.nio.ByteBuffer

class Tree(private val pageManager: PageManager, private val compare: KeyCompare) {
    companion object {
        val logicalMinimumKey = byteArrayOf().toByteBuffer()
        val logicalMaximumKey = null
    }

    private val rootNode: RootNode = RootNode(pageManager.getRootPage(), compare)
    private fun compare(a: ByteBuffer, b: ByteBuffer) = compare(a.toByteArray(), b.toByteArray())

    fun get(key: ByteBuffer): Record? {
        val result = findLeafNode(key)
        return result.leafNode.get(key)
    }

    fun put(key: ByteBuffer, value: ByteBuffer) {
        val result = findLeafNode(key)
        try {
            result.leafNode.put(key, value)
            result.leafNode.commit(pageManager)
        } catch (e: PageFullException) {
            splitNode(result.leafNode, result.pathFromRoot.reversed().iterator())
        }
    }

    fun delete(key: ByteBuffer) {
        val result = findLeafNode(key)
        result.leafNode.delete(key) // TODO merge
        result.leafNode.commit(pageManager)
    }

    fun get(record: Record) = get(record.key)
    fun put(record: Record) = put(record.key, record.value)
    fun delete(record: Record) = delete(record.key)

    fun scan(startKey: ByteBuffer? = logicalMinimumKey, endKey: ByteBuffer? = logicalMaximumKey): Sequence<Record> {
        if (startKey == endKey) return sequenceOf()
        val isAscending = when {
            startKey == logicalMinimumKey -> true
            startKey == logicalMaximumKey -> false
            endKey == logicalMinimumKey -> false
            endKey == logicalMaximumKey -> true
            else -> compare(startKey, endKey) < 0
        }
        val firstNode = findLeafNode(startKey).leafNode
        val lastNode = findLeafNode(endKey).leafNode
        return if (isAscending) {
            generateSequence(firstNode) { if(it == lastNode) null else createLeafNode(it.nextId) }
                .flatMap { it.records }
                .dropWhile { startKey != logicalMinimumKey && compare(it.key, startKey!!) < 0 } // it < startKey
                .takeWhile { endKey == logicalMaximumKey || compare(it.key, endKey) <= 0 }    // it <= endKey
        } else {
            generateSequence(firstNode) { if(it == lastNode) null else createLeafNode(it.previousId) }
                .flatMap { it.recordsReversed }
                .dropWhile { startKey != logicalMaximumKey && compare(it.key, startKey) > 0 } // it > startKey
                .takeWhile { endKey == logicalMinimumKey || compare(it.key, endKey!!) >= 0 }  // it >= endKey
        }
    }

    private fun createLeafNode(id: Int?): LeafNode? {
        val page = pageManager.get(id) ?: return null
        return LeafNode(page, compare)
    }

    private data class FindResult(val leafNode: LeafNode, val pathFromRoot: List<InternalNode>)

    private fun findLeafNode(
        key: ByteBuffer?,
        parentNode: InternalNode = rootNode, pathFromRoot: List<InternalNode> = listOf()): FindResult {
        if (parentNode.isLeafNode()) return FindResult(parentNode, pathFromRoot) // No root yet
        val newPathFromRoot = pathFromRoot + parentNode
        val childPageId = when(key) {
            logicalMinimumKey -> parentNode.firstChildPageId()
            logicalMaximumKey -> parentNode.lastChildPageId()
            else -> parentNode.findChildPageId(key)
        }
        val childPage = pageManager.get(childPageId) ?: throw Exception() // TODO
        return when (childPage.nodeType) {
            NodeType.LeafNode -> FindResult(LeafNode(childPage, compare), newPathFromRoot)
            NodeType.InternalNode -> findLeafNode(key, InternalNode(childPage, compare), newPathFromRoot)
            else -> throw Exception() // TODO
        }
    }

    private fun splitNode(node: Node, pathToRoot: Iterator<InternalNode>) {
        if (pathToRoot.hasNext()) {
            val parent = pathToRoot.next()
            val newNode = when (node.type) {
                NodeType.LeafNode -> LeafNode(node.split(pageManager), compare)
                NodeType.InternalNode -> InternalNode(node.split(pageManager), compare)
                else -> throw Exception() // TODO
            }
            newNode.commit(pageManager)
            try {
                parent.addChildNode(newNode)
                parent.commit(pageManager)
            } catch (e: PageFullException) {
                splitNode(parent, pathToRoot)
            }
        } else {
            splitRootNode()
        }
    }

    private fun splitRootNode() {
        val (leftPage, rightPage) = rootNode.splitRoot(pageManager)
        val (leftNode, rightNode) = when (rootNode.type) {
            NodeType.LeafNode -> Pair(LeafNode(leftPage, compare), LeafNode(rightPage, compare))
            NodeType.RootNode -> Pair(InternalNode(leftPage, compare), InternalNode(rightPage, compare))
            else -> throw Exception() // TODO
        }
        rootNode.addChildNode(leftNode, logicalMinimumKey)
        rootNode.addChildNode(rightNode)
        rootNode.type = NodeType.RootNode
        leftNode.commit(pageManager)
        rightNode.commit(pageManager)
        rootNode.commit(pageManager)
    }

    fun debug() {
        rootNode.printNode(pageManager)
    }
}
