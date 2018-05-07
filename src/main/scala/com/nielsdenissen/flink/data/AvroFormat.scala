package com.nielsdenissen.flink.data

import com.nielsdenissen.flink.managedstate.ManagedStateSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

import scala.collection.JavaConverters._
import scala.io.Source

trait AvroFormat[T] {
  def avroSchemaHistoryListString: List[String]
  def avroSchemaTargetString: String
  lazy val avroSchemaTarget: Schema = new Schema.Parser().parse(avroSchemaTargetString)

  def fromGenericRecord(gr: GenericRecord): T
  def toGenericRecord(t: T): GenericRecord

  def parseAvscFile(avscResourcePath: String): String = Source.fromURL(getClass.getResource(avscResourcePath)).mkString

  lazy val serializer: ManagedStateSerializer = new ManagedStateSerializer(avroSchemaHistoryListString, avroSchemaTargetString)

  protected def toAvroMap[VALUETYPE_IN, VALUETYPE_OUT](in: Map[String, VALUETYPE_IN], typeConverter: VALUETYPE_IN => VALUETYPE_OUT): java.util.Map[Utf8, VALUETYPE_OUT] =
    in.map(m => new Utf8(m._1) -> typeConverter(m._2)).asJava

  protected def fromAvroMap[VALUETYPE_IN, VALUETYPE_OUT](in: AnyRef, typeConverter: VALUETYPE_IN => VALUETYPE_OUT): Map[String, VALUETYPE_OUT] =
    in.asInstanceOf[java.util.Map[Utf8, VALUETYPE_IN]].asScala.toMap
      .map(m => m._1.toString -> typeConverter(m._2))
}
