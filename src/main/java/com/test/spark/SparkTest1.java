package com.test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class SparkTest1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext context = new JavaSparkContext("local", "test1", conf);
        JavaRDD<String> lines = context.textFile(args[0]);
        JavaRDD<String[]> records = lines.map(new Function<String, String[]>() {
            @Override
            public String[] call(String s) throws Exception {
                return s.split("\t");
            }
        });
        JavaPairRDD<String, Integer> pairs = records.mapToPair(new PairFunction<String[], String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String[] strings) throws Exception {
                return new Tuple2<>(strings[0], Integer.parseInt(strings[1]));
            }
        });
        JavaPairRDD<String, Integer> sum = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        sum.saveAsTextFile(args[1]);
    }
}
