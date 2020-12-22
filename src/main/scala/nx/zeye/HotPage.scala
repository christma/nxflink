package nx.zeye

import nx.utils.Utils
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object HotPage {

  def string2UserBehavior(line: String): UserBehavior = {

    val field = line.split(",")
    UserBehavior(field(0).trim.toLong, field(1).trim.toLong,
      field(2).trim.toLong, field(3).trim, field(4).trim.toLong, field(5).trim.toString)
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    env.readTextFile(Utils.USER_BEHAVIOR_PATH)
      .map(string2UserBehavior(_))
      .assignTimestampsAndWatermarks(new PageViewEventTimeExtractor)
      .filter(_.behavior == "P")
      .map(line => (line.behavior, 1))
      .timeWindowAll(Time.hours(1))
      .sum(1)
      .print()


    env.execute("HotPage")
  }
}

case class UserBehavior(
                         userId: Long,
                         productId: Long,
                         categoryId: Long,
                         behavior: String,
                         timeStamp: Long,
                         sessionId: String
                       )

class PageViewEventTimeExtractor extends AssignerWithPeriodicWatermarks[UserBehavior] {
  var currentMaxEventTime = 0L;
  val MaxOutOfOrderness = 10;

  override def getCurrentWatermark: Watermark = {
    new Watermark((currentMaxEventTime - MaxOutOfOrderness) * 1000)
  }

  override def extractTimestamp(t: UserBehavior, l: Long): Long = {
    val stamp = t.timeStamp
    currentMaxEventTime = Math.max(currentMaxEventTime, t.timeStamp)
    stamp
  }
}
