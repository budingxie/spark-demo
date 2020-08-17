package com.py;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

/**
 * @author pengyou@xiaomi.com
 * @date 2020/8/17
 */
public class AvgAgeJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("javaSpark");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = jsc.textFile(args[0]);
        Tuple2<Integer, Integer> tuple2 = rdd.map(new Function<String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(String s) throws Exception {
                return new Tuple2<Integer, Integer>(Integer.parseInt(s.split(" ")[1]), 1);
            }
        }).reduce(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> x, Tuple2<Integer, Integer> y) throws Exception {
                return new Tuple2<Integer, Integer>(x._1 + y._2, x._2 + y._2);
            }
        });
        System.out.println(Double.valueOf(tuple2._1) / Double.valueOf(tuple2._2));

    }
}
