package com.py

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author pengyou@xiaomi.com
 * @date 2020/8/17
 */
object AvgAge {

  def mai(args: Array[String]): Unit = {
    Array("1 15", "2 40", "3 30", "2 42", "2 48", "2 25")
    val conf: SparkConf = new SparkConf().setAppName("avaAge")
    val sc: SparkContext = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(0))

//    val f: ((Int, Int), (Int, Int)) => (Int, Int) = (x, y) => (x._1 + y._1, x._2 + y._2)
//    val tuple: (Int, Int) = lines.map(x => (x.split(" ")(1).toInt, 1)).reduce(f)
//    val ag: Double = tuple._1.toDouble / tuple._2.toDouble

    val count: Long = lines.count()
    val sumAge: Double = lines.map(x => x.split(" ")(1).toInt).sum()
    val avgAge: Double = sumAge / count
    println("平均年龄：", avgAge)
  }

}
