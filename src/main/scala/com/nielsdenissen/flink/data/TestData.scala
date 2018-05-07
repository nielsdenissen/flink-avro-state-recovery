package com.nielsdenissen.flink.data

import java.time.Instant

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.collection.JavaConverters._


case class TestData(
                     string: String,
                     int: Int,
                     bigDecimal: BigDecimal,
                     instant: Instant,
                     nested: TestDataNested,
                     option: Option[String],
                     list: List[String],
                     map: Map[String, TestDataNested]
                   )

object TestData extends AvroFormat[TestData] {
  def toGenericRecord(data: TestData): GenericRecord = {
    val genericRecord = new GenericData.Record(avroSchemaTarget)
    genericRecord.put("string", data.string)
    genericRecord.put("option", data.option.orNull)
    genericRecord.put("list",
      new GenericData.Array[String](Schema.createArray(Schema.create(Schema.Type.STRING)), data.list.asJava)
    )
    genericRecord.put("map", toAvroMap[TestDataNested, GenericRecord](data.map, TestDataNested.toGenericRecord))
    genericRecord.put("bigDecimal", data.bigDecimal.toString)
    genericRecord.put("nested", TestDataNested.toGenericRecord(data.nested))
    genericRecord.put("int", data.int)
    genericRecord.put("instant", data.instant.toEpochMilli)

    genericRecord
  }

  def fromGenericRecord(record: GenericRecord): TestData =
    TestData(
      string = record.get("string").toString,
      int = record.get("int").asInstanceOf[Int],
      bigDecimal = BigDecimal(record.get("bigDecimal").toString),
      instant = Instant.ofEpochMilli(record.get("timestamp").asInstanceOf[Long]),
      nested = TestDataNested.fromGenericRecord(record.get("nested").asInstanceOf[GenericRecord]),
      option = Option(record.get("option")).map(_.toString),
      list = record.get("list").asInstanceOf[GenericData.Array[String]].iterator().asScala.toList,
      map = fromAvroMap[GenericRecord, TestDataNested](record.get("map"), TestDataNested.fromGenericRecord)
   )

  val avroSchemaHistoryListString: List[String] = List()

  val avroSchemaTargetString: String = parseAvscFile("/avro/TestData0.avsc")
}