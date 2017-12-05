package com.dxhy.spark.solr

import com.dxhy.solr.dao.BaseSolrDao.generateInvoice4Solr
import com.dxhy.solr.dao.ClusterSolrDao
import com.dxhy.util.DataProcessUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by drguo on 2017/12/4.
  */
object Hdfs2Solr {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: Hdfs <directory>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("Hdfs2Solr")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[String]))
    val sc = new SparkContext(sparkConf)
    val rddStr: RDD[String] = sc.textFile(args(0))

    //val start = System.currentTimeMillis

    rddStr.filter(line => DataProcessUtil.jsonDataValidate(line)).repartition(36).foreach { line=>
      val invoice4Solr = generateInvoice4Solr(line)
      ClusterSolrDao.saveServer.addBean(invoice4Solr)
    }

    ClusterSolrDao.saveServer.commit()
    ClusterSolrDao.saveServer.shutdown()

    //println("cost:" + (System.currentTimeMillis - start) / 60000.0 + "mins.")//cost:1.92mins-13184条   28mins-805483条
    sc.stop()

  }
}
