package com.poype.bigdata.spark.second;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class MapOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        Integer[] numArray = {1, 2, 3, 4, 5, 6, 7, 8, 9};

        JavaRDD<Integer> dataRdd = sc.parallelize(Arrays.asList(numArray));

        // 这里我特意不使用lambda表达式，Function中的两个泛型一个是入参，一个是结果
        JavaRDD<Integer> resultRdd1 = dataRdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer num) {
                return num * 10;
            }
        });

        // 使用lambda表达式后的简化版本
        JavaRDD<Integer> resultRdd2 = dataRdd.map((Function<Integer, Integer>) num -> num * 20);

        System.out.println("resultRdd1: " + resultRdd1.collect());
        System.out.println("resultRdd2: " + resultRdd2.collect());
    }
}
