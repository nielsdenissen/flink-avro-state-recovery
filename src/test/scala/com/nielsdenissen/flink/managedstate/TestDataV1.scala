package com.nielsdenissen.flink.managedstate

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

case class TestDataV1(foo: String) {
  def this(record: GenericRecord) = this(record.get("foo").toString)

  lazy val toGenericRecord: GenericRecord = {
    val genericRecord = new GenericData.Record(schema)
    genericRecord.put("foo", foo)
    genericRecord
  }

  lazy val schema: Schema = new Schema.Parser().parse(TestDataV1.schemaString)
}

object TestDataV1 {
  def apply(record: GenericRecord) = new TestDataV1(record)
  val schemaString: String =
    """{
        "namespace": "testit",
        "type": "record",
        "name": "Version1",
        "fields": [
        { "name": "foo", "type": "string", "default": "" }
        ]
      }"""
}
