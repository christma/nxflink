package nx.cep

import java.util

import nx.cep.LoginCheckWithCEP.{LoginEvent, Warning}
import nx.utils.Utils
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time


object LoginCheckWithCEP {

  def string2UserLoginEvent(line: String) = {

    val dataArray = line.split(",")
    LoginEvent(dataArray(0).trim.toLong,
      dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)

  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)


    val loginEventStream = env.readTextFile(Utils.USER_LOGIN_LOG)
      .map(string2UserLoginEvent(_))
      .assignTimestampsAndWatermarks(new LoginCheckEventTimeExtractor())
      .keyBy(_.userId)


    val loginFailPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(3))


    val patternStream = CEP.pattern(loginEventStream, loginFailPattern)


    val loginFailDataStream = patternStream.select(new LoginFailMatch())


    loginEventStream.print()


    env.execute("LoginCheckWithCEP")
  }

  // 输入的登录事件样例类
  case class LoginEvent(userId: Long, //用户ID
                        ip: String, //IP地址
                        eventType: String, //用户登录行为，失败，成功两种状态
                        eventTime: Long) //事件时间
  // 输出的异常报警信息样例类
  case class Warning(userId: Long, //用户的ID
                     firstFailTime: Long, //第一次失败的时间
                     lastFailTime: Long, //第二次失败的时间
                     warningMsg: String) //告警内容
}

class LoginCheckEventTimeExtractor() extends AssignerWithPeriodicWatermarks[LoginEvent] {
  var currentEventTime = 0L
  var maxOutOfEvenTime = 10L

  override def getCurrentWatermark: Watermark = {
    new Watermark((currentEventTime - maxOutOfEvenTime) * 1000)
  }

  override def extractTimestamp(element: LoginEvent, l: Long): Long = {

    val timeStamp = element.eventTime * 1000

    currentEventTime = Math.max(currentEventTime, timeStamp)

    timeStamp
  }
}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    val firstFail = map.get("begin").iterator().next() //第一次失败的数据
    val lastFail = map.get("next").iterator().next() //第二次失败的数据
    Warning(firstFail.userId, firstFail.eventTime, lastFail.eventTime, "login fail!")
  }
}
