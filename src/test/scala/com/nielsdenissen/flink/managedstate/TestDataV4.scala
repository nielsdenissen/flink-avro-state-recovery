package com.nielsdenissen.flink.managedstate

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

case class TestDataV4(baz: String, foo: String) {
  def this(record: GenericRecord) = this(
    record.get("baz").toString,
    record.get("foo").toString
  )

  lazy val toGenericRecord: GenericRecord = {
    val genericRecord = new GenericData.Record(schema)
    genericRecord.put("baz", baz)
    genericRecord.put("foo", foo)
    genericRecord
  }

  lazy val schema: Schema = new Schema.Parser().parse(TestDataV4.schemaString)
}

object TestDataV4 {
  def apply(record: GenericRecord) = new TestDataV4(record)
  val schemaString: String =
    """{
        "namespace": "testit",
        "type": "record",
        "name": "Version4",
        "fields": [
        { "name": "baz", "type": ["string"], "default": "" },
        { "name": "foo", "type": "string", "default": "" }
        ]
      }"""

}

