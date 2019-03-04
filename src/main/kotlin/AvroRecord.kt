package com.riywo.ninja.bptree

import org.apache.avro.Schema
import org.apache.avro.generic.*
import org.apache.avro.io.*
import java.io.*

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
