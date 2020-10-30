package com.py

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author pengyou@xiaomi.com
 * @date 2020/8/17
 */
object PeopleInfoProc {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))

    //男性的数据rdd
    val maleData: RDD[(Int, Int)] = lines.filter(line => line.contains("M")).map(line => (1, line.split(" ")(2).toInt))
    //最高身高
    //最低身高
    //男性的总人数

    //女性的数据rdd
    val femaleData: RDD[(Int, Int)] = lines.filter(line => line.contains("F")).map(line => (1, line.split(" ")(2).toInt))
    //最高身高
    //最低身高
    //女性的总人数

    lines.map(line => {
      val value: Array[String] = line.split(" ")
      (value(1), value(2))
    }).groupByKey().foreach(line => {
      //将每组的value转化为list进行排序
      val arr: Iterable[String] = line._2
      val list: List[String] = arr.toList.sorted.reverse
      println(line._1 + "组最大值：" + list.head)
    })

  }
}
