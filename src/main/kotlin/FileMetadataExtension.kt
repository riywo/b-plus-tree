package com.riywo.ninja.bptree

import FileMetadata
import kotlin.reflect.KProperty

fun createFileMetadata(): FileMetadata {
    val builder = FileMetadata.newBuilder()
    builder.nextFreePageId = createPageId(ROOT_PAGE_ID)
    return builder.build()
}

val emptyFileMetadata = createFileMetadata()

enum class FileMetadataProperties {
    NextFreePageId
}

inline operator fun <reified T> FileMetadata.getValue(thisRef: Any, property: KProperty<*>): T {
    return when (FileMetadataProperties.valueOf(property.name.capitalize())) {
        FileMetadataProperties.NextFreePageId -> getNextFreePageId().getInt() as T
    }
}

operator fun <T> FileMetadata.setValue(thisRef: Any, property: KProperty<*>, value: T) {
    when (FileMetadataProperties.valueOf(property.name.capitalize())) {
        FileMetadataProperties.NextFreePageId -> getNextFreePageId().putInt(value as Int?)
        else -> throw Exception() // TODO
    }
}
