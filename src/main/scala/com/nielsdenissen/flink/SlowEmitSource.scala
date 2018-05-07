package com.nielsdenissen.flink

import java.time.Instant

import com.nielsdenissen.flink.data.{TestData, TestDataNested}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

class SlowEmitSource(intervalMs: Long) extends SourceFunction[TestData] with Serializable {
  private var isRunning: Boolean = true

  def generateData: TestData = TestData(
    string = "id",
    int = Random.nextInt(1000),
    bigDecimal = BigDecimal(Random.nextDouble()),
    instant = Instant.now(),
    nested = TestDataNested(1234),
    option = Some("option"),
    list = List("list"),
    map = Map("a" -> TestDataNested(0), "b" -> TestDataNested(1))
  )

  def run(ctx: SourceContext[TestData]) = {
    while (isRunning) {
      ctx.markAsTemporarilyIdle()
      Thread.sleep(intervalMs)
      ctx.collect(generateData)
    }
  }

  def cancel() = isRunning = false
}
