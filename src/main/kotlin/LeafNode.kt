package com.riywo.ninja.bptree

data class LeafNode(private val page: Page) : Page by page {
}
