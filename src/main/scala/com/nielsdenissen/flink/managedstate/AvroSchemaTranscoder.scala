package com.nielsdenissen.flink.managedstate

import akka.util.{ByteString, CompactByteString}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.{Schema, SchemaNormalization}

class AvroSchemaTranscoder(avroSchemaHistoryListString: List[String], avroSchemaTargetString: String) extends Serializable {
  // Transient makes sure this lazy val is not serialized and recalculated upon deserialization (The avro schema is not serializable)
  @transient private[this] lazy val avroSchemaMapping: Map[CompactByteString, Schema] =
    (avroSchemaHistoryListString :+ avroSchemaTargetString)
      .map(new Schema.Parser().parse(_))
      .map(s => getFingerprintFromSchema(s) -> s)
      .toMap
  @transient private[this] lazy val avroSchemaTarget: Schema = new Schema.Parser().parse(avroSchemaTargetString)

  // SHA-256 is on the list of mandatory JCE algorithms, so this shouldn't be an issue.
  private[this] def getFingerprintFromSchema(avroSchema: Schema): CompactByteString =
    ByteString(SchemaNormalization.parsingFingerprint("SHA-256", avroSchema)).compact

  /**
   * Encode the Avro Record into a Byte array:
   *
   * 1. A fingerprint of the schema (to determine later what schema was used to write)
   * 2. Encode the Avro schema
   *
   * @param record The record to encode
   * @return A byte array with the encoded record
   */
  def encode(record: GenericRecord): Array[Byte] = {
    val builder = ByteString.newBuilder

    // Add fingerprint of the schema
    builder.append(getFingerprintFromSchema(record.getSchema))

    // Encode the generic record
    val avroEncoder = EncoderFactory.get().binaryEncoder(builder.asOutputStream, null)
    new GenericDatumWriter[GenericRecord](record.getSchema).write(record, avroEncoder)
    avroEncoder.flush()

    builder.result().toArray
  }

  /**
   * Decode the Avro Record from a Byte array:
   *
   * 1. The fingerprint of the schema that was used to write
   * 2. Encoding of the Avro schema
   *
   * @param blob The byte Array with encoded Avro record
   * @return The decoded Generic Record
   */
  def decode(blob: Array[Byte]): GenericRecord = {
    val writeSchemaFingerprint = ByteString.fromArray(blob, 0, 32).compact
    val writeSchema = avroSchemaMapping.get(writeSchemaFingerprint)

    val decoder = DecoderFactory.get().binaryDecoder(blob, 32, blob.length - 32, null)

    val reader = writeSchema match {
      case Some(s) => new GenericDatumReader[GenericRecord](s, avroSchemaTarget)
      case None    => new GenericDatumReader[GenericRecord](avroSchemaTarget)
    }
    reader.read(null, decoder)
  }
}
