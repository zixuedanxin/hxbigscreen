package com.dxhy.spark.streaming

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.dxhy.log.CLogger
import com.dxhy.util.{DataProcessUtil, JDBCUtils}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 发票数据统计 | 同一企业nsrmc+nsrsbh 同一行业hydm x秒内开票量（张） 发票额 纳税额
  * Created by drguo on 2017/11/22.
  */
//com.dxhy.spark.streaming.InvoiceDataCount
object InvoiceDataCount extends CLogger {

  /**
    * 统计
    * @param ssc
    * @param topics
    * @param numThreads
    * @param zkQuorum
    * @param group
    */
  def runCount(ssc: StreamingContext, topics: String, numThreads: String, zkQuorum: String, group: String): Unit = {

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    lines.filter(line => DataProcessUtil.jsonDataValidate(line)).map { line =>
      val jsonData = JSON.parseObject(line)
      val jsonFP = jsonData.getJSONObject("Data").getJSONObject("FP_KJ")

      val nsrmc = jsonFP.getString("XSF_MC")
      val nsrsbh = jsonFP.getString("XSF_NSRSBH")
      val kprq = jsonFP.getString("KPRQ")
      val parse = new SimpleDateFormat("yyyyMMddHHmmss").parse(kprq)
      val formatKprq = new SimpleDateFormat("yyyy-MM-dd").format(parse)
      val fpdm = jsonFP.getString("FP_DM")
      var hydm = ""
      var hjje: Double = 0d
      var hjse: Double = 0d

      try {
        hydm = if (jsonFP.getString("HY_DM") == null || jsonFP.getString("HY_DM").length == 0) "#" else jsonFP.getString("HY_DM")
      } catch {
        case e: Exception => println(e)
        hydm = "#"
      }
      try {
        hjje = if (jsonFP.getString("HJJE").length == 0 || jsonFP.getString("HJJE") == null) 0d else jsonFP.getString("HJJE").toDouble
      }catch {
        case e: Exception => println(e)
        hjje = 0d
      }
      try {
        hjse = if (jsonFP.getString("HJSE").length == 0 || jsonFP.getString("HJSE") == null) 0d else jsonFP.getString("HJSE").toDouble
      } catch {
        case e: Exception => println(e)
        hjse = 0d
      }
      //Key: nsrmc nsrsbh 格式化后的kprq hydm | Value: fpdm kprq hjje hjse 1
      ((nsrmc, nsrsbh, formatKprq, hydm),(fpdm, kprq, hjje, hjse, 1))
    }.//reduceByKey((x, y) => (x._1, x._2, x._3+y._3, x._4+y._4, x._5+y._5))
      reduceByKey((value1, value2) => {
      val fpdm = value1._1
      val kprq = value1._2
      val hjje = value1._3+value2._3
      val hjse = value1._4+value2._4
      val cnt = value1._5+value2._5
      (fpdm,kprq,hjje,hjse,cnt)
    }).foreachRDD { rdd =>

      for (line <- rdd) {

        val key = line._1
        val value = line._2

        val fpdm = value._1
        val province_Number = fpdm.substring(1, 3)
        val city_Number = fpdm.substring(1, 5)
        val kprq = value._2
        val parse = new SimpleDateFormat("yyyyMMddHHmmss").parse(kprq)
        val formatKprq = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(parse)
        val kpje = value._3
        val kpse = value._4
        val cnt = value._5

        val nsrmc = key._1
        val nsrsbh = key._2
        val hydm = key._4
        //print(province_Number + "00", city_Number, fpdm, nsrmc, nsrsbh, formatKprq, kpje, kpse, cnt)
        info("nsrmc=" + nsrmc + "\tnsrsbh=" + nsrsbh + "\tcnt=" + cnt)
        JDBCUtils.insert2MapStat(province_Number + "00", city_Number, fpdm, nsrmc, nsrsbh, formatKprq, kpje, kpse, cnt) //大入口

        //JDBCUtils.insert2RegionCount(province_Number, formatKprq, cnt);//地图表

        //JDBCUtils.insert2FPHYProportion(hydm, cnt)

      }
    }
    ssc.start()
    ssc.awaitTermination()
  }


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("InvoiceDataCount")//.setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(10))//方块长度
    //ssc.checkpoint("checkpoint")

    val topics = "testinvoice"
    val numThreads = "2"
    val zkQuorum = "DXHY-YFEB-01:2181,DXHY-YFEB-02:2181,DXHY-YFEB-03:2181"
    val group = "51fp"

    runCount(ssc, topics, numThreads, zkQuorum, group)
  }
}

