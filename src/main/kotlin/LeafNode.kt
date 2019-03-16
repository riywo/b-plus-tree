package com.riywo.ninja.bptree

import java.nio.ByteBuffer

class LeafNode(table: Table, page: AvroPage) : Node(table.key, table.record, page) {
}
