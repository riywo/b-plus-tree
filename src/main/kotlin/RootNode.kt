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

class RootNode(page: Page, compare: KeyCompare) : InternalNode(page, compare) {
    fun splitRoot(pageManager: PageManager): Pair<Page, Page> {
        val newType = if (type == NodeType.RootNode) NodeType.InternalNode else NodeType.LeafNode
        val leftPage = pageManager.move(page, newType, 0 until recordsSize)
        val rightPage = pageManager.split(leftPage)
        return Pair(leftPage, rightPage)
    }
}