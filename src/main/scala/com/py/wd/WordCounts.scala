package com.py.wd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * description 
 *
 * @author pengyou@xiaomi.com
 * @date 2020/10/29
 * @version 1.0.0
 */
object WordCounts {

  def main(args: Array[String]): Unit = {
    // 1.创建Spark Context
    // 2.读取文件并计算词频
    // 3.查看结果

    // 提交spark集群
    val conf: SparkConf = new SparkConf().setAppName("wd-local")
    // 本地测试
//    val conf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("wd-local")
    val sc: SparkContext = new SparkContext(conf)
    // 读取hdfs文件
    val source: RDD[String] = sc.textFile("hdfs://linux1:9000/testFile/foo.txt", 2)
    // 读取本地文件
//    val source: RDD[String] = sc.textFile("dataset/wordcount.txt", 2)
    val words: RDD[String] = source.flatMap(line => line.split(" "))
    val wordsTuple: RDD[(String, Int)] = words.map(word => (word, 1))
    val wordsCount: RDD[(String, Int)] = wordsTuple.reduceByKey((x, y) => x + y)
    val result: Array[(String, Int)] = wordsCount.collect()
    result.foreach(item => println(item))
  }

}
