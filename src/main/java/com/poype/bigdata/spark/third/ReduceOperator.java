package com.poype.bigdata.spark.third;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;

public class ReduceOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Integer[] nums = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        JavaRDD<Integer> dataRdd = sc.parallelize(Arrays.asList(nums));
        int sum = dataRdd.reduce((Function2<Integer, Integer, Integer>) Integer::sum);

        // sum: 55
        System.out.println("sum: " + sum);
    }
}
