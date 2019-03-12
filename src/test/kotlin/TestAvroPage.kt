package com.riywo.ninja.bptree

import org.junit.jupiter.api.*
import org.assertj.core.api.Assertions.*

import org.apache.avro.SchemaBuilder

class TestAvroPage {
    private val schema = SchemaBuilder.builder().record("foo").fields()
        .name("key").type().stringType().noDefault()
        .name("value").orderIgnore().type().stringType().noDefault()
        .endRecord()
    private val table = Table(schema)

    private var page: Page = AvroPage.new(table, 0)
    private val record = table.Record()

    init {
        record.put("key", "1")
        record.put("value", "a")
        page.put(table.key.encode(record), table.record.encode(record))
    }

    @BeforeEach
    fun init() {
        page = AvroPage.new(table, 0)
        page.put(table.key.encode(record), table.record.encode(record))
        assertThat(page.records().size).isEqualTo(1)
        assertThat(page.size()).isEqualTo(page.dump().limit())
    }

    @Test
    fun `dump and load`() {
        val pageLoaded = AvroPage.load(table, page.dump())
        assertThat(pageLoaded.id()).isEqualTo(page.id())
        assertThat(pageLoaded.size()).isEqualTo(page.size())
        assertThat(pageLoaded.dump()).isEqualTo(page.dump())
    }

    @Test
    fun `get record`() {
        val found = page.get(table.key.encode(record))
        assertThat(found).isEqualTo(table.record.encode(record))
        assertThat(page.records().size).isEqualTo(1)
    }

    @Test
    fun `insert record`() {
        val recordInserted = table.Record()
        recordInserted.put("key", "2")
        recordInserted.put("value", "b")
        page.put(table.key.encode(recordInserted), table.record.encode(recordInserted))

        assertThat(page.records().size).isEqualTo(2)
        assertThat(page.get(table.key.encode(record))).isEqualTo(table.record.encode(record))
        assertThat(page.get(table.key.encode(recordInserted))).isEqualTo(table.record.encode(recordInserted))
        assertThat(page.size()).isEqualTo(page.dump().limit())
    }

    @Test
    fun `update record`() {
        val recordUpdated = table.Record()
        recordUpdated.put("key", "1")
        recordUpdated.put("value", "b")
        page.put(table.key.encode(recordUpdated), table.record.encode(recordUpdated))

        assertThat(page.records().size).isEqualTo(1)
        assertThat(page.get(table.key.encode(record))).isEqualTo(table.record.encode(recordUpdated))
        assertThat(page.get(table.key.encode(recordUpdated))).isEqualTo(table.record.encode(recordUpdated))
        assertThat(page.size()).isEqualTo(page.dump().limit())
    }

    @Test
    fun `delete record`() {
        page.delete(table.key.encode(record))

        assertThat(page.records().size).isEqualTo(0)
        assertThat(page.get(table.key.encode(record))).isEqualTo(null)
        assertThat(page.size()).isEqualTo(page.dump().limit())
    }

    @Test
    fun `can't put record`() {
        val newRecord = table.Record()
        newRecord.put("key", "2")
        newRecord.put("value", "a".repeat(MAX_PAGE_SIZE))
        assertThrows<PageFullException> {
            page.put(table.key.encode(newRecord), table.record.encode(newRecord))
        }
        assertThat(page.records().size).isEqualTo(1)
        assertThat(page.get(table.key.encode(record))).isEqualTo(table.record.encode(record))
        assertThat(page.size()).isEqualTo(page.dump().limit())
    }
}