package com.nielsdenissen.flink.managedstate

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

case class TestDataV3(foo: Int) {
  def this(record: GenericRecord) = this(record.get("foo").toString.toInt)

  lazy val toGenericRecord: GenericRecord = {
    val genericRecord = new GenericData.Record(schema)
    genericRecord.put("foo", foo)
    genericRecord
  }

  lazy val schema: Schema = new Schema.Parser().parse(TestDataV3.schemaString)
}

object TestDataV3 {
  def apply(record: GenericRecord) = new TestDataV3(record)
  val schemaString: String =
    """{
        "namespace": "testit",
        "type": "record",
        "name": "Version1",
        "fields": [
        { "name": "foo", "type": ["string","int"], "default": "" }
        ]
      }"""

}

