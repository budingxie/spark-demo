package com.py

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author pengyou@xiaomi.com
 * @date 2020/8/14
 */
object Demo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("scala_word_count")
    val sc: SparkContext = new SparkContext(conf)
    val path: String = "/tmp/loacal-file/word.txt"
    //按照行读取文件
    val lines: RDD[String] = sc.textFile(path)
    //根据每一行空格分割，并扁平化
    val word: RDD[String] = lines.flatMap(_.split(" "))
    //按照单词映射
    //val fMap: String => (String, Int) = x => (x, 1)
    //val wordAndCount: RDD[(String, Int)] = word.map(fMap)
    val wordAndCount: RDD[(String, Int)] = word.map((_, 1))
    //统计单词
    val reduced: RDD[(String, Int)] = wordAndCount.reduceByKey(_ + _)
    //按照统计结果倒叙
    val sortWordCount: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    val f: ((String, Int)) => Unit = (x) => println(x._1, x._2)
    //保存
    sortWordCount.saveAsTextFile("/tmp/loacal-file/scala-word")
    //关闭资源
    sc.stop()
  }

}
