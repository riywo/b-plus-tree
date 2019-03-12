package com.riywo.ninja.bptree

open class Base {
    companion object {
        fun new(): Base {
            return Base()
        }
    }
}

class Foo : Base() {
    companion object {
        fun new() = Base.new() as Foo
    }
}

class Bar : Base() {
    companion object {
        fun new() = Base.new() as Bar
    }
}

fun main() {
    val foo = Foo.new()
    val bar = Bar.new()
    println(foo)
    println(bar)
}
