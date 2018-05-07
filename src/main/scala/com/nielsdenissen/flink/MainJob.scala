package com.nielsdenissen.flink

import com.nielsdenissen.flink.processor.CalcDiffFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object MainJob {

  def main(args: Array[String]) {
    val flinkEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    flinkEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    flinkEnvironment.addSource(new SlowEmitSource(1000))
      .assignAscendingTimestamps(_.instant.toEpochMilli)
      .keyBy(_.string)
      .process(CalcDiffFunction())
      .name("Calc diff")
      .uid("calc-diff-function")
      .print()

    flinkEnvironment.execute()
  }
}
