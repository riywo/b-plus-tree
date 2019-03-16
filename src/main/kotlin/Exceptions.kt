package com.riywo.ninja.bptree

class PageFullException(message: String) : Exception(message)

class NodeAlreadyInitializedException(message: String) : Exception(message)

class NodeNotInitializedException(message: String) : Exception(message)

class InternalNodeInitializeException(message: String) : Exception(message)
