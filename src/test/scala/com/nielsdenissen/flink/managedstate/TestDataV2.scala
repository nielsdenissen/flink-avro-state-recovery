package com.nielsdenissen.flink.managedstate

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

case class TestDataV2(foo: String, bar: Int) {
  def this(record: GenericRecord) = this(
    record.get("foo").toString,
    record.get("bar").toString.toInt
  )

  lazy val toGenericRecord: GenericRecord = {
    val genericRecord = new GenericData.Record(schema)
    genericRecord.put("foo", foo)
    genericRecord.put("bar", bar)

    genericRecord
  }

  lazy val schema: Schema = new Schema.Parser().parse(TestDataV2.schemaString)
}

object TestDataV2 {
  def apply(record: GenericRecord) = new TestDataV2(record)
  val schemaString: String =
    """{
        "namespace": "testit",
        "type": "record",
        "name": "Version1",
        "fields": [
        { "name": "foo", "type": "string", "default": "pop" },
        { "name": "bar", "type": ["int","string"], "default": 53 }
        ]
      }"""
}
