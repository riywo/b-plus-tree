package com.riywo.ninja.bptree

import java.nio.ByteBuffer

class PageFullException(message: String) : Exception(message)

class KeyBytesMismatchException(message: String) : Exception(message)
