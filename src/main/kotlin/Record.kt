package com.riywo.ninja.bptree

import org.apache.avro.Schema
import org.apache.avro.generic.*
import org.apache.avro.io.*
import java.io.*

fun main() {
    val schema = Schema.Parser().parse("""
        {
            "name": "foo",
            "type": "record",
            "fields": [
                {"name": "bar", "type": "int"},
                {"name": "baz", "type": "string", "order": "ignore"}
            ]
        }
    """.trimIndent())
    val table = Table(schema)

    val record1 = table.Record()
    record1.put("bar", 1)
    record1.put("baz", "hello")
    val data = record1.encode()
    val record2 = table.Record()
    record2.decode(data)
    println(record2)
    println(table.Key().decode(data))
}

data class Table(val schema: Schema) {
    val keySchema: Schema by lazy {
        Schema.createRecord(keyFields)
    }

    private val keyFields by lazy {
        schema.fields
            .takeWhile { it.order() != Schema.Field.Order.IGNORE }
            .map { Schema.Field(it.name(), it.schema(), it.doc(), it.defaultVal(), it.order()) }
    }

    inner class Record : AvroRecord(schema, AvroRecord.IO(schema))
    inner class Key : AvroRecord(keySchema, AvroRecord.IO(keySchema))
}

open class AvroRecord(schema: Schema, private val io: IO) : GenericData.Record(schema) {
    fun encode(): ByteArray {
        return io.encode(this)
    }

    fun decode(data: ByteArray): AvroRecord {
        return io.decode(this, data)
    }

    class IO(schema: Schema) {
        private val writer = GenericDatumWriter<GenericRecord>(schema)
        private val reader = GenericDatumReader<GenericRecord>(schema)

        private var encoder: BinaryEncoder? = null
        private var decoder: BinaryDecoder? = null

        fun encode(record: AvroRecord): ByteArray {
            val output = ByteArrayOutputStream()
            encoder = EncoderFactory.get().binaryEncoder(output, encoder)
            writer.write(record, encoder)
            encoder?.flush()
            return output.toByteArray()
        }

        fun decode(record: AvroRecord, data: ByteArray): AvroRecord {
            decoder = DecoderFactory.get().binaryDecoder(data, decoder)
            reader.read(record, decoder)
            return record
        }
    }
}
