package com.atguigu.hot

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItem {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop102:9092")
    properties.setProperty("groupId","consumer-group")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val dataStream: DataStream[UserBehavior] = env.addSource(new FlinkKafkaConsumer[String]("hotItem", new SimpleStringSchema(), properties))
      .map(
        line => {
          val arr = line.split(",")
          UserBehavior(arr(0).trim.toLong, arr(1).trim.toLong, arr(2).trim.toInt, arr(3).trim, arr(4).trim.toLong)
        }
      )
    dataStream
      .filter(_.behavior=="pv")
      .assignAscendingTimestamps(_.timestamp*1000)
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1),Time.minutes(5))
      .aggregate(new AggregateView,new WindowResult)
      .keyBy(_.windowEnd)
      .process(new TopN(3))
      .print()

   // dataStream.print("input")
    env.execute("hotTopN")

  }

}

class AggregateView extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = {
    0L
  }

  override def add(in: UserBehavior, acc: Long): Long = {
    acc + 1L
  }

  override def getResult(acc: Long): Long = {
    acc
  }

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowResult extends WindowFunction[Long,ItemCount,Long,TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemCount]): Unit = {
    val itemId = key
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemCount(itemId,windowEnd,count))
  }
}

class TopN(nSize: Int) extends KeyedProcessFunction[Long,ItemCount,String]{

  private var listState : ListState[ItemCount] = _
 // private var timeState : ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    listState = getRuntimeContext.getListState(new ListStateDescriptor[ItemCount]("liststate",classOf[ItemCount]))
    //timeState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeState",classOf[Long]))
  }

  override def processElement(i: ItemCount, context: KeyedProcessFunction[Long, ItemCount, String]#Context, collector: Collector[String]): Unit = {
    listState.add(i)
    context.timerService().registerEventTimeTimer(i.windowEnd + 100)




  }
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    val listbuffer : ListBuffer[ItemCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for(i <- listState.get()){
      listbuffer += i
    }

    val counts: ListBuffer[ItemCount] = listbuffer.sortBy(_.count)(Ordering.Long.reverse).take(nSize)

    val builder = new StringBuilder

    builder.append("===================================\n")
    builder.append("时间：  ").append(new Timestamp(timestamp - 100)).append("\n")
    for (elem <- counts.indices) {
      builder.append("No").append(elem + 1)
        .append("商品:").append(counts(elem).itemId)
        .append("浏览量:").append(counts(elem).count).append("\n")
    }

    builder.append("========================\n")
    Thread.sleep(1000)
    out.collect(builder.toString())




  }
}
