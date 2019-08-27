package com.atguigu.workFlow


import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.{SimpleTimerService, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val dataStream: DataStream[String] = env.readTextFile("D:\\尚硅谷学习\\39_flink\\代码\\UserBehaviorAnalysis\\NetworkFlow\\src\\main\\resources\\apache.log")
    dataStream.map(data=>{
      val arr = data.split(" ")
      val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val time = format.parse(arr(3)).getTime
      ApacheLogEvent(arr(0).trim,arr(1).trim,time,arr(5).trim,arr(6).trim)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds(1)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = {
          t.eventTime
        }
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10),Time.seconds(5))
      .aggregate(new AggFunction,new ResultFunction)
      .keyBy(_.windowEnd)
      .process(new TopNView(3))
      .print()

    env.execute("flow")
  }
}

class AggFunction extends AggregateFunction[ApacheLogEvent,Long,Long]{
  override def createAccumulator(): Long = {
    0L
  }

  override def add(in: ApacheLogEvent, acc: Long): Long = {
    acc + 1L
  }

  override def getResult(acc: Long): Long = {
    acc
  }

  override def merge(acc: Long, acc1: Long): Long = {
    acc + acc1
  }
}

class ResultFunction extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key,window.getEnd,input.iterator.next()))
  }
}

class TopNView(nSize : Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{
  lazy val listState : ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("liststate",classOf[UrlViewCount]))
  override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
    listState.add(i)
    context.timerService().registerEventTimeTimer(i.windowEnd + 1L)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allUrlViews : ListBuffer[UrlViewCount] = ListBuffer()

    /*while(listState.get().iterator().hasNext){
      allUrlViews += listState.get().iterator().next()
    }*/

    import scala.collection.JavaConversions._
    for (urlView <- listState.get) {
      allUrlViews += urlView
    }
    listState.clear()

    val viewCounts = allUrlViews.sortWith((x, y) => {
      x.count > y.count
    }).take(nSize)

    var result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- viewCounts.indices) {
      val currentUrlView: UrlViewCount = viewCounts(i)
      // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
      result.append("No").append(i+1).append(":")
        .append("  URL=").append(currentUrlView.url)
        .append("  流量=").append(currentUrlView.count).append("\n")
    }
    result.append("====================================\n\n")
    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)
    out.collect(result.toString)
  }

}
