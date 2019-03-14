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

    private var page: Page = AvroPage.new(table.key, table.record, 1)
    private val record = table.Record()

    init {
        record.put("key", "1")
        record.put("value", "a")
        page.put(record)
    }

    @BeforeEach
    fun init() {
        page = AvroPage.new(table.key, table.record, 1)
        page.put(record)
        assertThat(page.id).isEqualTo(1)
        assertThat(page.sentinelId).isEqualTo(null)
        assertThat(page.previousId).isEqualTo(null)
        assertThat(page.nextId).isEqualTo(null)
        assertThat(page.records.size).isEqualTo(1)
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `dump and load`() {
        val pageLoaded = AvroPage.load(table.key, table.record, page.dump())
        assertThat(pageLoaded.id).isEqualTo(page.id)
        assertThat(pageLoaded.size).isEqualTo(page.size)
        assertThat(pageLoaded.dump()).isEqualTo(page.dump())
    }

    @Test
    fun `get record`() {
        val found = page.get(record)
        assertThat(found).isEqualTo(record)
        assertThat(page.records.size).isEqualTo(1)
    }

    @Test
    fun `insert record`() {
        val recordInserted = table.Record()
        recordInserted.put("key", "2")
        recordInserted.put("value", "b")
        page.put(recordInserted)

        assertThat(page.records.size).isEqualTo(2)
        assertThat(page.get(record)).isEqualTo(record)
        assertThat(page.get(recordInserted)).isEqualTo(recordInserted)
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `update record`() {
        val recordUpdated = table.Record()
        recordUpdated.put("key", "1")
        recordUpdated.put("value", "b")
        page.put(recordUpdated)

        assertThat(page.records.size).isEqualTo(1)
        assertThat(page.get(record)).isEqualTo(recordUpdated)
        assertThat(page.get(recordUpdated)).isEqualTo(recordUpdated)
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `delete record`() {
        page.delete(record)

        assertThat(page.records.size).isEqualTo(0)
        assertThat(page.get(record)).isEqualTo(null)
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `can't put record`() {
        val newRecord = table.Record()
        newRecord.put("key", "2")
        newRecord.put("value", "a".repeat(MAX_PAGE_SIZE))
        assertThrows<PageFullException> {
            page.put(newRecord)
        }
        assertThat(page.records.size).isEqualTo(1)
        assertThat(page.get(record)).isEqualTo(record)
        assertThat(page.size).isEqualTo(page.dump().limit())
    }

    @Test
    fun `sentinelId mutation`() {
        assertThat(page.sentinelId).isEqualTo(null)
        page.sentinelId = 1
        assertThat(page.sentinelId!!).isEqualTo(1)
        val pageLoaded = AvroPage.load(table.key, table.record, page.dump())
        assertThat(pageLoaded.id).isEqualTo(page.id)
        assertThat(pageLoaded.size).isEqualTo(page.size)
        assertThat(pageLoaded.dump()).isEqualTo(page.dump())
        assertThat(pageLoaded.sentinelId).isEqualTo(page.sentinelId!!)
    }

    @Test
    fun `put lots of records`() {
        val num = 100
        for (i in 2..num) {
            val newRecord = table.Record()
            newRecord.put("key", "$i")
            newRecord.put("value", "$i")
            page.put(newRecord)
        }
        assertThat(page.records.size).isEqualTo(num)
        assertThat(page.size).isEqualTo(page.dump().limit())
    }
}