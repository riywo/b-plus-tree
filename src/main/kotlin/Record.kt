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

import java.nio.ByteBuffer
import KeyValue

data class Record(val key: ByteBuffer, val value: ByteBuffer) {
    constructor(keyValue: KeyValue) : this(keyValue.getKey(), keyValue.getValue())
    constructor(key: ByteArray, value: ByteArray) : this(key.toByteBuffer(), value.toByteBuffer())
    constructor(key: ByteBuffer, value: ByteArray) : this(key, value.toByteBuffer())
    constructor(key: ByteArray, value: ByteBuffer) : this(key.toByteBuffer(), value)
}
