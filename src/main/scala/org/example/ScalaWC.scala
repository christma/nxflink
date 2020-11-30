package org.example

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object ScalaWC {
  def main(args: Array[String]): Unit = {
    val hostname = ParameterTool.fromArgs(args).get("hostname")
    val port = ParameterTool.fromArgs(args).get("port").toInt


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream = env.socketTextStream(hostname, port)
    val wc = dataStream.flatMap(_.split(","))
      .map((_, 1)).keyBy(0).sum(1)
    wc.print()

    env.execute("ScalaWC")
  }

}
