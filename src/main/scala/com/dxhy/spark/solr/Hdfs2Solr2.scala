package com.dxhy.spark.solr

import com.dxhy.util.DataProcessUtil
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 比Hdfs2Solr慢
  * Created by drguo on 2017/12/4.
  */
object Hdfs2Solr2 {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Error-Pls Usage: Hdfs <directory>")
      System.exit(1)
    }
    val zkHost: String = "DXHY-YFEB-01:2181,DXHY-YFEB-02:2181,DXHY-YFEB-03:2181/solr"
    val defaultCollection: String = "test_invoice2"
    val sparkConf = new SparkConf().setAppName("Hdfs2Solr2")

    //kyro序列化
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[String]))
    val sc = new SparkContext(sparkConf)

    val rddStr: RDD[String] = sc.textFile(args(0))
    val start = System.currentTimeMillis


    rddStr.filter(line => DataProcessUtil.jsonDataValidate(line)).repartition(24).foreachPartition {

      partition => {
        val cloudServer = new CloudSolrServer(zkHost)
        cloudServer.setZkClientTimeout(7000)
        cloudServer.setZkConnectTimeout(3000)
        cloudServer.setDefaultCollection(defaultCollection)
        cloudServer.connect()

        partition.foreach {
          line =>
            val invoice4Solr = DataProcessUtil.generateInvoice4Solr(line)
            cloudServer.addBean(invoice4Solr)
        }
        cloudServer.commit()
        cloudServer.shutdown()
      }
      /*foreach { line=>
      val invoice4Solr = generateInvoice4Solr(line)
      ClusterSolrDao.saveServer.addBean(invoice4Solr)}*/
    }

    println("cost:" + (System.currentTimeMillis - start) / 60000.0 + "mins.") //cost:2.025-13184  5.9 mins-805483条
    sc.stop()

  }
}
