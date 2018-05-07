package com.nielsdenissen.flink.managedstate

import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeutils._
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.util.InstantiationUtil

class ManagedStateSerializer(avroSchemaHistoryListString: List[String], avroSchemaTargetString: String)
    extends TypeSerializer[GenericRecord]
    with Serializable {
  /*
    Avro state serialization part
   */
  @transient lazy val avroSchemaTranscoder = new AvroSchemaTranscoder(
    avroSchemaHistoryListString = avroSchemaHistoryListString,
    avroSchemaTargetString = avroSchemaTargetString
  )

  override def serialize(record: GenericRecord, target: DataOutputView): Unit = {
    val blob = avroSchemaTranscoder.encode(record)
    target.writeInt(blob.length)
    target.write(blob)
  }

  override def deserialize(source: DataInputView): GenericRecord = {
    val blobSize = source.readInt()
    val blob = new Array[Byte](blobSize)
    source.read(blob)
    avroSchemaTranscoder.decode(blob)
  }

  /*
    Default functions required for TypeSerializer
   */

  override def createInstance(): GenericRecord = InstantiationUtil.instantiate(classOf[GenericRecord])

  override def canEqual(obj: scala.Any): Boolean = obj.isInstanceOf[ManagedStateSerializer]

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case _ => false
    }
  }

  override def duplicate(): TypeSerializer[GenericRecord] =
    new ManagedStateSerializer(avroSchemaHistoryListString, avroSchemaTargetString)

  override def ensureCompatibility(configSnapshot: TypeSerializerConfigSnapshot): CompatibilityResult[GenericRecord] =
    CompatibilityResult.compatible[GenericRecord]()

  override def hashCode(): Int = 1

  override def isImmutableType: Boolean = false

  override def getLength: Int = -1

  override def snapshotConfiguration(): TypeSerializerConfigSnapshot =
    new ParameterlessTypeSerializerConfig("identifier")

  override def copy(from: GenericRecord): GenericRecord = from

  override def copy(from: GenericRecord, reuse: GenericRecord): GenericRecord = copy(from)

  override def copy(source: DataInputView, target: DataOutputView): Unit = serialize(deserialize(source), target)

  override def deserialize(reuse: GenericRecord, source: DataInputView): GenericRecord = deserialize(source)
}
