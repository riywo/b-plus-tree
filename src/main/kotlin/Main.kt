package com.riywo.ninja.bptree

import java.io.File
import com.github.ajalt.clikt.core.*
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import org.apache.avro.Schema
import org.jline.reader.*
import java.lang.Exception

class Main(file: File) {
    private val fileManager = FileManager.load(file)
    private val pageManager = PageManager(fileManager)
    private val table = Table(fileManager.keySchema, fileManager.valueSchema)
    private val tree = Tree(pageManager, table.key::compare, table.minimumKey.toByteBuffer())
    private val reader = LineReaderBuilder.builder().build()

    init {
        reader.setVariable(LineReader.HISTORY_FILE, ".history")
        reader.history.attach(reader)
        reader.history.load()
    }

    fun run() {
        val prompt = "COMMAND: "
        while (true) {
            try {
                when (reader.readLine(prompt)) {
                    "get" -> get()
                    "put" -> put()
                    "delete" -> delete()
                    "scan" -> scan()
                }
            } catch (e: UserInterruptException) {
                // Ignore
            } catch (e: EndOfFileException) {
                return
            } catch (e: Exception) {
                System.err.println(e)
            }
            println("")
        }
    }

    private fun get() {
        val key = readKey() ?: return
        val result = tree.get(key.toByteBuffer())
        if (result == null) {
            println("NOTFOUND")
        } else {
            val record = table.createRecord(result.key, result.value)
            println("${record.key} ${record.value}")
        }
    }

    private fun put() {
        val key = readKey() ?: return
        val value = readValue() ?: return
        tree.put(key.toByteBuffer(), value.toByteBuffer())
    }

    private fun delete() {
        val key = readKey() ?: return
        tree.delete(key.toByteBuffer())
    }

    private fun scan() {
        val startKey = readKey("START KEY: ") ?: return
        val endKey = readKey("END KEY: ") ?: return
        val results = tree.scan(startKey.toByteBuffer(), endKey.toByteBuffer())
        for (result in results) {
            val record = table.createRecord(result.key, result.value)
            println("${record.key} ${record.value}")
        }
    }

    private fun readKey(prompt: String = "KEY: "): Table.Key? {
        return try {
            table.createKey(reader.readLine(prompt))
        } catch (e: Exception) {
            System.err.println(e)
            null
        }
    }

    private fun readValue(prompt: String = "VALUE: "): Table.Value? {
        return try {
            table.createValue(reader.readLine(prompt))
        } catch (e: Exception) {
            System.err.println(e)
            null
        }
    }
}

class CLI : CliktCommand() {
    override fun run() = Unit

    class Init : CliktCommand() {
        private val filePath by argument()
        private val keySchemaPath by option().default("sample/keySchema.avsc")
        private val valueSchemaPath by option().default("sample/valueSchema.avsc")

        override fun run() {
            val file = File(filePath)
            if (file.exists()) {
                throw BadParameterValue("$filePath already exists.")
            } else {
                val keySchema = Schema.Parser().parse(File(keySchemaPath))
                val valueSchema = Schema.Parser().parse(File(valueSchemaPath))
                val table = Table(keySchema, valueSchema)
                FileManager.new(file, keySchema, valueSchema)
            }
        }
    }

    class Open : CliktCommand() {
        private val filePath by argument()

        override fun run() {
            val file = File(filePath)
            if (file.exists()) {
                val main = Main(file)
                main.run()
            } else {
                throw BadParameterValue("$filePath doesn't exist.")
            }
        }
    }
}

fun main(args: Array<String>) = CLI()
    .subcommands(CLI.Init(), CLI.Open())
    .main(args)
