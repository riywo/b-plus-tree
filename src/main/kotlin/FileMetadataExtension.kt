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

import FileMetadata
import kotlin.reflect.KProperty

fun createFileMetadata(): FileMetadata {
    val builder = FileMetadata.newBuilder()
    builder.nextFreePageId = createPageId(ROOT_PAGE_ID)
    return builder.build()
}

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
