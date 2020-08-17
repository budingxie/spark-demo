package com.py;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * @author pengyou@xiaomi.com
 * @date 2020/8/14
 */
public class JavaDemo {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("java-spark");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("/tmp/loacal-file/word.txt");
        JavaRDD<String> map = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String value) throws Exception {
                return Stream.of(value.split(" ")).iterator();
            }
        });
        //map
        JavaPairRDD<String, Integer> wordAndOne = map.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        //reduce
        JavaPairRDD<String, Integer> reduce = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        //调换顺序
        JavaPairRDD<Integer, String> reduced = reduce.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.swap();
            }
        });
        //排序
        JavaPairRDD<Integer, String> sort = reduced.sortByKey(false);
        //调换顺序
        JavaPairRDD<String, Integer> finalText = sort.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                return tuple2.swap();
            }
        });
        //保存
        finalText.saveAsTextFile("/tmp/loacal-file/java-word");
        jsc.stop();
    }

}
