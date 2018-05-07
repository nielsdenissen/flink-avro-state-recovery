package com.nielsdenissen.flink.processor

import com.nielsdenissen.flink.data.TestData
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

case class CalcDiffFunction() extends ProcessFunction[TestData, TestData] {
  val LOG: Logger = LoggerFactory.getLogger("calcDiffFunction")

  lazy val bufferState: ValueState[GenericRecord] =
    getRuntimeContext.getState(new ValueStateDescriptor[GenericRecord]("calcDiff", TestData.serializer))

  def getValue: Option[TestData] = Option(bufferState.value()).map(TestData.fromGenericRecord)
  def setValue(v: TestData): Unit = bufferState.update(TestData.toGenericRecord(v))

  private def calcDiff(oldData: TestData, newData: TestData): TestData = {
    newData.copy(
      int = newData.int - oldData.int,
      bigDecimal = newData.bigDecimal - oldData.bigDecimal
    )
  }

  /**
    * Calculate the difference of the 2 case classes for some fields and send that out.
    * Then set the state to the latest incoming case class.
    */
  override def processElement(
                               in: TestData,
                               ctx: ProcessFunction[TestData, TestData]#Context,
                               collector: Collector[TestData]
  ): Unit = {
    getValue
      .foreach(od => collector.collect(calcDiff(od, in)))
    setValue(in)
  }
}
