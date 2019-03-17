package com.riywo.ninja.bptree

import PageData
import NodeType
import java.nio.ByteBuffer
import java.lang.Exception
import kotlin.reflect.KProperty

enum class PageDataProperties {
    Id, PreviousId, NextId, NodeType
}

fun createPageData(id: Int, nodeType: NodeType): PageData {
    val builder = PageData.newBuilder()
    builder.id = createPageId(id)
    builder.nodeType = nodeType
    builder.previousId = createPageId()
    builder.nextId = createPageId()
    builder.records = mutableListOf<ByteBuffer>()
    return builder.build()
}

inline operator fun <reified T> PageData.getValue(thisRef: Any, property: KProperty<*>): T {
    return when (PageDataProperties.valueOf(property.name.capitalize())) {
        PageDataProperties.Id -> getId().getInt() as T
        PageDataProperties.NodeType -> getNodeType() as T
        PageDataProperties.PreviousId -> getPreviousId().getInt() as T
        PageDataProperties.NextId -> getNextId().getInt() as T
        else -> throw Exception() // TODO
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
