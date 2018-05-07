package com.nielsdenissen.flink.managedstate

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.apache.avro.generic.GenericRecord
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

trait ManagedStateSerializerTestHelper {
  private class mockOutput(out: ByteArrayOutputStream) extends DataOutputStream(out) with DataOutputView {
    override def skipBytesToWrite(numBytes: Int) = ???

    override def write(source: DataInputView, numBytes: Int) = ???
  }

  private class mockInput(in: ByteArrayInputStream) extends DataInputStream(in) with DataInputView {
    override def skipBytesToRead(numBytes: Int) = ???
  }

  def getSerDeser(ser: ManagedStateSerializer, deser: ManagedStateSerializer) =
    (gr: GenericRecord) => {
      val out = new ByteArrayOutputStream
      ser.serialize(gr, new mockOutput(out))
      deser.deserialize(new mockInput(new ByteArrayInputStream(out.toByteArray)))
    }
}
