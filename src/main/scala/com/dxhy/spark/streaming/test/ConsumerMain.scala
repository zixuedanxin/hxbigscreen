package com.dxhy.spark.streaming.test


import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import kafka.serializer.StringDecoder

/**
 * @author drguo
 */
object ConsumerMain extends Serializable {
  @transient lazy val log = LogManager.getRootLogger
  def functionToCreateContext(): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("WordFreqConsumer")
//      .set("spark.streaming.kafka.maxRatePerPartition", "10")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // Create direct kafka stream with brokers and topics
    val topicsSet = "test".split(",").toSet
    val kafkaParams = scala.collection.immutable.Map[String, String]("metadata.broker.list" -> "DXHY-YFEB-01:9092,DXHY-YFEB-02:9092,DXHY-YFEB-03:9092", "auto.offset.reset" -> "smallest", "group.id" -> "TestConsumerGroup")
    val km = new KafkaManager(kafkaParams)
    val kafkaDirectStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    log.warn(s"Initial Done***>>>")

    kafkaDirectStream.cache

    val words = kafkaDirectStream.map(_._2).flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).reduceByKey(_+_)
    wordCounts.print()

    //do something......

    //更新zk中的offset
    kafkaDirectStream.foreachRDD(rdd => {
      if (!rdd.isEmpty)
        km.updateZKOffsets(rdd)
    })

    ssc
  }


  def main(args: Array[String]) {
    val ssc = functionToCreateContext()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}