package com.nielsdenissen.flink.managedstate

import org.scalatest.{DiagrammedAssertions, FlatSpec}

class ManagedStateSerializerTest extends FlatSpec with DiagrammedAssertions with ManagedStateSerializerTestHelper {
  val serializerVersion1 = new ManagedStateSerializer(List(), TestDataV1.schemaString)
  val serializerVersion2 = new ManagedStateSerializer(List(TestDataV1.schemaString), TestDataV2.schemaString)
  val serializerVersion3 = new ManagedStateSerializer(List(TestDataV1.schemaString), TestDataV3.schemaString)

  "Managed State serializer" should "serialize and deserialize a case class as generic record" in {
    val msgIn = TestDataV1("foo")

    val grOut = getSerDeser(serializerVersion1, serializerVersion1)(msgIn.toGenericRecord)
    val msgOut = TestDataV1(grOut)

    assertResult(msgIn)(msgOut)
  }

  it should "evolve data (extra field) through serialize and deserialize a case class as generic record" in {
    val msgIn = TestDataV1("foo")

    val grOut = getSerDeser(serializerVersion1, serializerVersion2)(msgIn.toGenericRecord)
    val msgOut = TestDataV2(grOut)

    assertResult(TestDataV2(foo = "foo", bar = 53))(msgOut)
  }

  it should "evolve data (remove field) through serialize and deserialize a case class as generic record" in {
    val msgIn = TestDataV2(foo = "foo", bar = 53)

    val grOut = getSerDeser(serializerVersion2, new ManagedStateSerializer(List(TestDataV2.schemaString), TestDataV1.schemaString))(msgIn.toGenericRecord)
    val msgOut = TestDataV1(grOut)

    assertResult(TestDataV1("foo"))(msgOut)
  }

  it should "handle simple schema evolution (type conversion)" in {
    val msgIn = TestDataV1("10")

    val grOut = getSerDeser(serializerVersion1, serializerVersion3)(msgIn.toGenericRecord)
    val msgOut = TestDataV3(grOut)

    assertResult(TestDataV3(10))(msgOut)
  }

  // This case used to go wrong, when we remove data the old generic record be written by the new serializer with the
  // new schema. This schema doesn't fit the data though. So upon write, always write with the schema attached to the GR
  it should "be able to write old generic records in case data didn't change (new serializer writes old generic record)" in {
    val msgIn = TestDataV4(baz = "baz", foo = "foo")

    val ser = new ManagedStateSerializer(List(TestDataV4.schemaString), TestDataV1.schemaString)
    val grOut = getSerDeser(ser, ser)(msgIn.toGenericRecord)
    val msgOut = TestDataV1(grOut)

    assertResult(TestDataV1("foo"))(msgOut)
  }
}

