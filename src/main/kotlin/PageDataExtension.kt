package com.riywo.ninja.bptree

import PageData
import java.nio.ByteBuffer
import java.lang.Exception
import kotlin.reflect.KProperty

enum class PageDataProperties {
    Id, SentinelId, PreviousId, NextId
}

fun createPageData(id: Int): PageData {
    val builder = PageData.newBuilder()
    builder.id = createPageId(id)
    builder.sentinelId = createPageId()
    builder.previousId = createPageId()
    builder.nextId = createPageId()
    builder.records = mutableListOf<ByteBuffer>()
    return builder.build()
}

inline operator fun <reified T> PageData.getValue(thisRef: Any, property: KProperty<*>): T {
    return when (PageDataProperties.valueOf(property.name.capitalize())) {
        PageDataProperties.Id -> getId().getInt()
        PageDataProperties.SentinelId -> getSentinelId().getInt()
        PageDataProperties.PreviousId -> getPreviousId().getInt()
        PageDataProperties.NextId -> getNextId().getInt()
    } as T
}

operator fun <T> PageData.setValue(thisRef: Any, property: KProperty<*>, value: T) {
    when (PageDataProperties.valueOf(property.name.capitalize())) {
        PageDataProperties.SentinelId -> getSentinelId().putInt(value as Int?)
        PageDataProperties.PreviousId -> getPreviousId().putInt(value as Int?)
        PageDataProperties.NextId -> getNextId().putInt(value as Int?)
        else -> throw Exception()
    }
}

