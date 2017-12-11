package com.dxhy.spark.hbase

import com.dxhy.util.DataProcessUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
/**
  * Created by drguo on 2017/12/6.
  */
object Hdfs2HBase {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Error-Pls Usage: Hdfs <directory>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("Hdfs2HBase")
    val sc = new SparkContext(sparkConf)

    val tablename = "test:Invoice2"

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","DXHY-YFEB-01,DXHY-YFEB-02,DXHY-YFEB-03")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val rddStr = sc.textFile(args(0))

    //val start = System.currentTimeMillis

    val rdd = rddStr.filter(line => DataProcessUtil.jsonDataValidate(line)).map { line =>
      val put = DataProcessUtil.generatePut(line)
      (new ImmutableBytesWritable, put)
    }

    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
    //println("cost:" + (System.currentTimeMillis - start) / 60000.0 + "mins.")//1.3万  cost:0.3448mins. 80万+ 9.1 min
    sc.stop()
  }
}
