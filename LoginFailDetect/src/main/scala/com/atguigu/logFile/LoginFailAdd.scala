package com.atguigu.logFile

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object LoginFailAdd {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      //LoginEvent(1, "192.168.0.3", "success", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime*1000)
      .keyBy(_.userId)
      .process(new LoginAdd)
      .print()
    env.execute()
  }

}


class LoginAdd extends KeyedProcessFunction[Long,LoginEvent,String]{
  lazy val listState : ListState[LoginEvent]= getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("listState",classOf[LoginEvent]))
  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, String]#Context, collector: Collector[String]): Unit = {
    if (i.eventType == "fail") {
      val ite = listState.get().iterator()
      if(ite.hasNext){
        val firstFail = ite.next()
        if(i.eventTime > firstFail.eventTime + 2){
          listState.clear()
          listState.add(i)
        }else{
          collector.collect(firstFail.eventTime + "," + i.eventTime + "," + "连续两次登录失败")
        }
      }else{
        listState.add(i)
      }
    }else{
      listState.clear()
    }
  }

}
