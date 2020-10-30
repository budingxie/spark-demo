//package com.py.useranays.runapp;
//
//import com.py.useranays.conf.ConfigurationManager;
//import com.py.useranays.conf.Constants;
//import com.py.useranays.simdata.MockData;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.SQLContext;
//
//
///**
// * 用户访问session分析Spark作业
// *
// * @author pengyou@xiaomi.com
// * @date 2020/8/25
// */
//public class UserVisitSessionAnalyzeSpark {
//
//    public static void main(String[] args) {
//        //构建Spark上下文
//        SparkConf sparkConf = new SparkConf();
//        //Spark作业本地运行
//        sparkConf.setMaster("local");
//        //为了符合大型企业的开发需求，不能出现硬编码，创建一个Constants接口类，定义一些常量
//        sparkConf.setAppName(Constants.SPARK_APP_NAME_SESSION);
//
//        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//        SQLContext sqlContext = new SQLContext(jsc);
//
//        mockData(jsc, sqlContext);
//        jsc.stop();
//    }
//
//
//    /**
//     * 生成模拟数据（只有本地模式，才会去生成模拟数据）
//     *
//     * @param sc
//     * @param sqlContext
//     */
//    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
//        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
//        if (local) {
//            MockData.mock(sc, sqlContext);
//        }
//    }
//}
