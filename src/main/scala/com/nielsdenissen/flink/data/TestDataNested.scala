package com.nielsdenissen.flink.data

import org.apache.avro.generic.{GenericData, GenericRecord}

case class TestDataNested(
  entry: Int
)

object TestDataNested extends AvroFormat[TestDataNested] {
  def toGenericRecord(data: TestDataNested): GenericRecord = {
    val genericRecord = new GenericData.Record(avroSchemaTarget)
    genericRecord.put("entry", data.entry)

    genericRecord
  }

  def fromGenericRecord(record: GenericRecord): TestDataNested = {
    TestDataNested(
      entry = record.get("entry").asInstanceOf[Int]
    )
  }

  val avroSchemaHistoryListString: List[String] = List()

  val avroSchemaTargetString: String = parseAvscFile("/avro/TestDataNested0.avsc")
}
