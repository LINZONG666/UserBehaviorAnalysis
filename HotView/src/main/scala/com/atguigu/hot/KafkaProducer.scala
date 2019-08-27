package com.atguigu.hot

import java.util.Properties

import com.atguigu.hot.KafkaProducer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}




object KafkaProducer {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotItem")
  }

  def writeToKafka(topic: String): Unit ={
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop102:9092")

    properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String,String](properties)

    val bufferedSource = io.Source.fromFile("D:\\尚硅谷学习\\39_flink\\代码\\UserBehaviorAnalysis\\HotView\\src\\main\\resources\\UserBehavior.csv")
    for(line <- bufferedSource.getLines()){
      val record = new ProducerRecord[String,String](topic,line)
      producer.send(record)
    }

    producer.close()
  }


}

