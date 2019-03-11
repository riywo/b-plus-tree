package com.riywo.ninja.bptree

import org.apache.avro.Schema
import org.apache.avro.generic.*
import org.apache.avro.io.*
import java.io.*

class AvroRecordIO(val schema: Schema) {
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

    fun write(record: GenericRecord, output: ByteArrayOutputStream){
        encoder = encoderFactory.binaryEncoder(output, encoder)
        writer.write(record, encoder)
        encoder?.flush()
    }

    fun write(record: GenericRecord): ByteArray {
        val output = ByteArrayOutputStream()
        write(record, output)
        return output.toByteArray()
    }

    fun read(record: GenericRecord, input: ByteArrayInputStream) {
        decoder = decoderFactory.binaryDecoder(input, decoder)
        reader.read(record, decoder)
    }

    fun read(record: GenericRecord, bytes: ByteArray) {
        val input = ByteArrayInputStream(bytes)
        read(record, input)
    }

    fun read(bytes: ByteArray): GenericRecord {
        val record = GenericData.Record(schema)
        read(record, bytes)
        return record
    }

    fun compare(a: ByteArray, b: ByteArray): Int {
        return BinaryData.compare(a, 0 , b, 0, schema)
    }
}