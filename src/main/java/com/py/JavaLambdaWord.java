package com.py;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.stream.Stream;

/**
 * @author pengyou@xiaomi.com
 * @date 2020/8/14
 */
public class JavaLambdaWord {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("java-spark");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("/tmp/loacal-file/word.txt");
        JavaRDD<String> map = lines.flatMap((FlatMapFunction<String, String>) value -> Stream.of(value.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordAndOne = map.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<String, Integer>(s, 1));
        JavaPairRDD<String, Integer> reduce = wordAndOne.reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2);
        JavaPairRDD<Integer, String> reduced = reduce.mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) tuple2 -> tuple2.swap());
        //排序
        JavaPairRDD<Integer, String> sort = reduced.sortByKey(false);
        JavaPairRDD<String, Integer> finalText = sort.mapToPair((PairFunction<Tuple2<Integer, String>, String, Integer>) tuple2 -> tuple2.swap());
        //保存
        finalText.saveAsTextFile("/tmp/loacal-file/java-word");
        jsc.stop();
    }
}
