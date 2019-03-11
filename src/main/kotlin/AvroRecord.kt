package com.riywo.ninja.bptree

import org.apache.avro.Schema
import org.apache.avro.generic.*
import org.apache.avro.io.*
import java.io.*

open class AvroRecord(private val io: IO) : GenericData.Record(io.schema) {
    fun load(input: ByteArrayInputStream) {
        io.decode(this, input)
    }

    fun load(bytes: ByteArray) {
        io.decode(this, bytes)
    }

    class IO(val schema: Schema) {
        init {
            if (schema.type != Schema.Type.RECORD) {
                throw IllegalArgumentException("Schema type must be record: $schema")
            }
        }

        private val writer = GenericDatumWriter<GenericRecord>(schema)
        private val reader = GenericDatumReader<GenericRecord>(schema)

        private val encoderFactory = EncoderFactory.get()
        private val decoderFactory = DecoderFactory.get()

        private var encoder: BinaryEncoder? = null
        private var decoder: BinaryDecoder? = null

        fun encode(record: GenericRecord, output: ByteArrayOutputStream){
            encoder = encoderFactory.binaryEncoder(output, encoder)
            writer.write(record, encoder)
            encoder?.flush()
        }

        fun encode(record: GenericRecord): ByteArray {
            val output = ByteArrayOutputStream()
            encode(record, output)
            return output.toByteArray()
        }

        fun decode(record: GenericRecord, input: ByteArrayInputStream) {
            decoder = decoderFactory.binaryDecoder(input, decoder)
            reader.read(record, decoder)
        }

        fun decode(record: GenericRecord, bytes: ByteArray) {
            val input = ByteArrayInputStream(bytes)
            decode(record, input)
        }

        fun compare(a: ByteArray, b: ByteArray): Int {
            return BinaryData.compare(a, 0 , b, 0, schema)
        }
    }
}