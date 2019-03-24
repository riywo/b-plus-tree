package com.riywo.ninja.bptree

import java.nio.ByteBuffer

typealias KeyCompare = (ByteArray, ByteArray) -> Int

typealias MergeRule = (new: ByteBuffer, old: ByteBuffer) -> ByteBuffer