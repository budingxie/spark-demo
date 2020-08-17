package com.py;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * @author pengyou@xiaomi.com
 * @date 2020/8/17
 */
public class AvgAgeJavaLambda {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("javaLambdaSpark");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = jsc.textFile(args[0]);
        Tuple2<Integer, Integer> tuple2 = rdd.map(line -> new Tuple2<>(Integer.parseInt(line.split(" ")[1]), 1))
                .reduce((x, y) -> new Tuple2<>(x._1 + y._2, x._2 + y._2));
        System.out.println(Double.valueOf(tuple2._1) / Double.valueOf(tuple2._2));
    }
}
