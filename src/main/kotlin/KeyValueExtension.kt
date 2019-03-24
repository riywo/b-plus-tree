package com.riywo.ninja.bptree

import KeyValue

fun KeyValue.toAvroBytesSize(): Int {
    return getKey().toAvroBytesSize() + getValue().toAvroBytesSize()
}
