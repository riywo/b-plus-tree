package com.riywo.ninja.bptree

import org.apache.avro.Schema
import org.apache.avro.generic.*
import org.apache.avro.io.*
import org.apache.avro.message.*
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

open class AvroGenericRecord(private val io: IO) : GenericData.Record(io.schema) {
    fun load(byteBuffer: ByteBuffer) {
        io.decode(this, byteBuffer)
    }

    fun toByteBuffer(): ByteBuffer {
        return io.encode(this)
    }

    class IO(val schema: Schema) {
        init {
            if (schema.type != Schema.Type.RECORD) {
                throw IllegalArgumentException("Schema type must be record: $schema")
            }
        }

        private val writer = GenericDatumWriter<GenericRecord>(schema)
        private val reader = GenericDatumReader<GenericRecord>(schema)

        private var encoder: BinaryEncoder? = null
        private var decoder: BinaryDecoder? = null

        fun encode(record: AvroGenericRecord): ByteBuffer {
            val output = ByteArrayOutputStream()
            encoder = EncoderFactory.get().binaryEncoder(output, encoder)
            writer.write(record, encoder)
            encoder?.flush()
            return output.toByteArray().toByteBuffer()
        }

        fun decode(record: AvroGenericRecord, byteBuffer: ByteBuffer) {
            decoder = DecoderFactory.get().binaryDecoder(byteBuffer.toByteArray(), decoder)
            reader.read(record, decoder)
        }

        fun compare(a: ByteArray, b: ByteArray): Int {
            return BinaryData.compare(a, 0 , b, 0, schema)
        }

        fun compare(a: ByteBuffer, b: ByteBuffer): Int {
            return compare(a.toByteArray(), b.toByteArray())
        }

        fun compare(a: AvroGenericRecord, b: AvroGenericRecord): Int {
            return compare(a.toByteBuffer(), b.toByteBuffer())
        }
    }
}