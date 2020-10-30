package com.py.acceslog

import org.apache.commons.lang3.StringUtils
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

  @Test
  def parseLog(): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("ip_ana")
    val sc: SparkContext = new SparkContext(conf)
    val result: Array[(String, Int)] = sc.textFile("dataset/access_log_sample.txt")
      .map(item => (item.split(" ")(0), 1))
      .filter(item => StringUtils.isNotBlank(item._1))
      .reduceByKey((curr, agg) => curr + agg)
      .sortBy(item => item._2, false)
      .take(10)
    result.foreach(item=> println(item))
  }

}
