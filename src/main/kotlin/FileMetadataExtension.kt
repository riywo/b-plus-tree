package com.riywo.ninja.bptree

import FileMetadata
import kotlin.reflect.KProperty
import org.apache.avro.Schema

fun createFileMetadata(keySchema: Schema, valueSchema: Schema): FileMetadata {
    val builder = FileMetadata.newBuilder()
    builder.nextFreePageId = createPageId(ROOT_PAGE_ID)
    builder.keySchema = keySchema.toString()
    builder.valueSchema = valueSchema.toString()
    return builder.build()
}

enum class FileMetadataProperties {
    NextFreePageId, KeySchema, ValueSchema
}

inline operator fun <reified T> FileMetadata.getValue(thisRef: Any, property: KProperty<*>): T {
    return when (FileMetadataProperties.valueOf(property.name.capitalize())) {
        FileMetadataProperties.NextFreePageId -> getNextFreePageId().getInt() as T
        FileMetadataProperties.KeySchema -> Schema.Parser().parse(getKeySchema()) as T
        FileMetadataProperties.ValueSchema -> Schema.Parser().parse(getValueSchema()) as T
    }
}

operator fun <T> FileMetadata.setValue(thisRef: Any, property: KProperty<*>, value: T) {
    when (FileMetadataProperties.valueOf(property.name.capitalize())) {
        FileMetadataProperties.NextFreePageId -> getNextFreePageId().putInt(value as Int?)
        else -> throw Exception() // TODO
    }
}
