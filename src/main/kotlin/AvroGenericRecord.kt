package com.riywo.ninja.bptree

import org.apache.avro.Schema
import org.apache.avro.generic.*
import org.apache.avro.io.*
import org.apache.avro.message.*
import java.io.*
import java.nio.ByteBuffer

open class AvroGenericRecord(private val io: IO) : GenericData.Record(io.schema) {
    fun load(input: ByteArrayInputStream) {
        io.decode(this, input)
    }

    fun load(byteBuffer: ByteBuffer) {
        io.decode(this, byteBuffer)
    }

    class IO(val schema: Schema) {
        init {
            if (schema.type != Schema.Type.RECORD) {
                throw IllegalArgumentException("Schema type must be record: $schema")
            }
        }

        private val encoder = BinaryMessageEncoder<GenericRecord>(GenericData(), schema)
        private val decoder = BinaryMessageDecoder<GenericRecord>(GenericData(), schema)

        fun encode(record: GenericRecord, output: OutputStream) {
            encoder.encode(record, output)
        }

        fun encode(record: GenericRecord): ByteBuffer {
            return encoder.encode(record)
        }

        fun decode(record: GenericRecord, input: InputStream) {
            decoder.decode(input, record)
        }

        fun decode(record: GenericRecord, byteBuffer: ByteBuffer) {
            decoder.decode(byteBuffer, record)
        }

        fun compare(a: ByteArray, b: ByteArray): Int {
            return BinaryData.compare(a, 0 , b, 0, schema)
        }
    }
}