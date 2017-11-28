package com.dxhy.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 准实时单词统计
  * Created by drguo on 2017/11/22.
  */
object WordCount {

  def run(ssc: StreamingContext, topics: String, numThreads: String, zkQuorum: String, group: String): Unit = {

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordCounts = words.map((_, 1)).reduceByKey(_+_)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    /*
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum, group, topics, numThreads) = args
    */
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))//方块长度
    //ssc.checkpoint("checkpoint")

    val topics = "test"
    val numThreads = "2"
    val zkQuorum = "DXHY-YFEB-01:2181,DXHY-YFEB-02:2181,DXHY-YFEB-03:2181"
    val group = "TestConsumerGroup"

    run(ssc, topics, numThreads, zkQuorum, group)
  }

}
