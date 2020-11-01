package com.py.acceslog

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
 * description 
 *
 * @author pengyou@xiaomi.com
 * @date 2020/10/30
 * @version 1.0.0
 */
class AccessLog {

  val conf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("SCF")
  val sc: SparkContext = new SparkContext(conf)

  @Test
  def parseLog(): Unit = {

    val result: Array[(String, Int)] = sc.textFile("dataset/access_log_sample.txt")
      .map(item => (item.split(" ")(0), 1))
      .filter(item => StringUtils.isNotBlank(item._1))
      .reduceByKey((curr, agg) => curr + agg)
      .sortBy(item => item._2, false)
      .take(10)

    result.foreach(item => println(item))
  }

  @Test
  def mapPartitionsWithIndex(): Unit = {
    val array: Array[String] = sc.textFile("dataset/wordcount.txt")
      .mapPartitionsWithIndex((index, item) => {
        println("index：" + index)
        item.foreach(ele => print(ele + "=="))
        item
      })
      .collect()
    println(array)
  }

  @Test
  def mapPartitions(): Unit = {
    sc.parallelize(Seq(1, 2, 3, 4, 56, 7), 2)
      .mapPartitions(item => {
        item.map(ele => ele * 10)
      }).collect()
      .foreach(ele => println(ele))
  }

  @Test
  def filter(): Unit = {
    sc.parallelize(Seq(1, 2, 3, 4, 5, 6))
      .filter(ele => ele % 2 == 0)
      .collect()
      .foreach(ele => println(ele))
  }

  @Test
  def sample(): Unit = {
    // 1.定义数据集
    // 2.过滤数据
    // 3.收集结果

    sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8))
      .sample(true, 0.6)
      .collect()
      .foreach(ele => println(ele))
  }

  @Test
  def mapValues(): Unit = {
    sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      .mapValues(value => value * 10)
      .collect()
      .foreach(ele => println(ele))
  }

  @Test
  def intersection(): Unit = {
    val rdd1: RDD[Int] = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val rdd2: RDD[Int] = sc.parallelize(Seq(3, 4, 5, 6, 7))

    rdd1.intersection(rdd2)
      .collect()
      .foreach(ele => println(ele))
  }

  @Test
  def union(): Unit = {
    val rdd1: RDD[Int] = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val rdd2: RDD[Int] = sc.parallelize(Seq(3, 4, 5, 6, 7))

    rdd1.union(rdd2)
      .collect()
      .foreach(println(_))
  }

  @Test
  def subtract(): Unit = {
    val rdd1: RDD[Int] = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val rdd2: RDD[Int] = sc.parallelize(Seq(3, 4, 5, 6, 7))

    rdd1.subtract(rdd2)
      .collect()
      .foreach(println(_))
  }


}
