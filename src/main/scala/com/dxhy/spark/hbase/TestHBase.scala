package com.dxhy.spark.hbase

import com.dxhy.constants.HBaseParams
import com.dxhy.hbase.dao.BaseHBaseDao
import com.dxhy.util.DataProcessUtil
import org.apache.spark._
import org.apache.spark.rdd.RDD

/**
  * 没有调用spark-hbase api 慢很多
  */
object TestHBase {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Error-Pls Usage: Hdfs <directory>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("TestHdfs2HBase")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[String]))
    val sc = new SparkContext(sparkConf)
    val rddStr: RDD[String] = sc.textFile(args(0))

    val start = System.currentTimeMillis

    rddStr.filter(line => DataProcessUtil.jsonDataValidate(line)).foreachPartition { partition =>{
      val table = BaseHBaseDao.getTable(HBaseParams.INVOICE_TABLE_NAME_VALUE)
      partition.foreach{ line =>
        val put = BaseHBaseDao.generatePut(line)
        if (put != null) {
          BaseHBaseDao.saveInvoice(table, put)
        }
      }
      BaseHBaseDao.close(table)
    }
    }

    println("cost:" + (System.currentTimeMillis - start) / 60000.0 + "mins.")//1.3万 cost:1.6668mins.  80万+ cost:17mins.
    sc.stop()

  }

}  