package com.atguigu.logFile

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object LoginFailCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
/*    val value = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      //LoginEvent(1, "192.168.0.3", "success", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))*/
      val value = env.socketTextStream("hadoop102",7777)
      .map(data=>{
        val arr = data.split(",")
        LoginEvent(arr(0).toLong,arr(1),arr(2),arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(1)) {
        override def extractTimestamp(t: LoginEvent): Long = {
          t.eventTime*1000
        }
      })
      .keyBy(_.userId)
    val loginFailPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(20))
    val patternStream = CEP.pattern(value,loginFailPattern)

    val loginFailDataStream = patternStream.select(new LoginFailMatch)
    loginFailDataStream.print()

    value.print("input")


//


    env.execute()
  }

}

class LoginFailMatch extends PatternSelectFunction[LoginEvent,String]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): String = {
    val firstFail = map.get("begin").iterator().next()
    val secondFail = map.get("next").iterator().next()
    firstFail.eventTime + "," + secondFail.eventTime + "," + "连续失败两次"
  }
}

