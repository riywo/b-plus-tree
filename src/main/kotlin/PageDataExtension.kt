package com.riywo.ninja.bptree

import PageData
import NodeType
import KeyValue
import java.lang.Exception
import kotlin.reflect.KProperty

fun createPageData(id: Int, nodeType: NodeType, initialRecords: MutableList<KeyValue>): PageData {
    val builder = PageData.newBuilder()
    builder.id = createPageId(id)
    builder.nodeType = nodeType
    builder.previousId = createPageId()
    builder.nextId = createPageId()
    builder.records = initialRecords
    return builder.build()
}

val emptyPageData = createPageData(-1, NodeType.LeafNode, mutableListOf())

enum class PageDataProperties {
    Id, PreviousId, NextId, NodeType
}

inline operator fun <reified T> PageData.getValue(thisRef: Any, property: KProperty<*>): T {
    return when (PageDataProperties.valueOf(property.name.capitalize())) {
        PageDataProperties.Id -> getId().getInt() as T
        PageDataProperties.NodeType -> getNodeType() as T
        PageDataProperties.PreviousId -> getPreviousId().getInt() as T
        PageDataProperties.NextId -> getNextId().getInt() as T
    }
}

operator fun <T> PageData.setValue(thisRef: Any, property: KProperty<*>, value: T) {
    when (PageDataProperties.valueOf(property.name.capitalize())) {
        PageDataProperties.NodeType -> setNodeType(value as NodeType)
        PageDataProperties.PreviousId -> getPreviousId().putInt(value as Int?)
        PageDataProperties.NextId -> getNextId().putInt(value as Int?)
        else -> throw Exception() // TODO
    }
}
