package com.nielsdenissen.flink.managedstate

import org.scalatest.{DiagrammedAssertions, FlatSpec}

class AvroSchemaTranscoderTest extends FlatSpec with DiagrammedAssertions {
  "Avro Schema Transcoder" should "encode and decode a case class as generic record" in {
    val ast = new AvroSchemaTranscoder(
      avroSchemaHistoryListString = List(TestDataV1.schemaString),
      avroSchemaTargetString = TestDataV1.schemaString
      )
    val msgIn = TestDataV1("foo")

    val grOut = ast.decode(ast.encode(msgIn.toGenericRecord))
    val msgOut = TestDataV1(grOut)

    assertResult(msgIn)(msgOut)
  }

  it should "evolve data (extra field) through encoding and decoding a case class as generic record" in {
    val ast1 = new AvroSchemaTranscoder(
      avroSchemaHistoryListString = List(TestDataV1.schemaString),
      avroSchemaTargetString = TestDataV1.schemaString
    )
    val ast2 = new AvroSchemaTranscoder(
      avroSchemaHistoryListString = List(TestDataV1.schemaString, TestDataV2.schemaString),
      avroSchemaTargetString = TestDataV2.schemaString
    )
    val msgIn = TestDataV1("foo")

    val grOut = ast2.decode(ast1.encode(msgIn.toGenericRecord))
    val msgOut = TestDataV2(grOut)

    assertResult(TestDataV2(foo = "foo", bar = 53))(msgOut)
  }

  it should "handle simple schema evolution (type conversion)" in {
    val ast1 = new AvroSchemaTranscoder(
      avroSchemaHistoryListString = List(TestDataV1.schemaString),
      avroSchemaTargetString = TestDataV1.schemaString
    )
    val ast2 = new AvroSchemaTranscoder(
      avroSchemaHistoryListString = List(TestDataV1.schemaString),
      avroSchemaTargetString = TestDataV3.schemaString
    )
    val msgIn = TestDataV1("10")

    val grOut = ast2.decode(ast1.encode(msgIn.toGenericRecord))
    val msgOut = TestDataV3(grOut)

    assertResult(TestDataV3(10))(msgOut)
  }
}
