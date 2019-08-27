package com.atguigu.logFile


import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)
object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime*1000)
      .keyBy(_.userId)
      .process(new Login)
      .print()
    env.execute()
  }

}

class Login extends KeyedProcessFunction[Long,LoginEvent,String]{
  lazy val listState : ListState[LoginEvent]= getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("listState",classOf[LoginEvent]))
  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, String]#Context, collector: Collector[String]): Unit = {
    if(i.eventType=="fail"){
      listState.add((i))
    }
    context.timerService().registerEventTimeTimer(i.eventTime*1000 + 2000)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
    val listBuff : ListBuffer[LoginEvent]=ListBuffer()

    import scala.collection.JavaConversions._
    for( i <- listState.get()){
      listBuff += i
    }
    listState.clear()

    if(listBuff.length>1){
      val firstTime = listBuff.head.eventTime
      val lastTime = listBuff.last.eventTime
      out.collect(firstTime + "," + lastTime + "," + "连续失败两次")
    }
  }
}
