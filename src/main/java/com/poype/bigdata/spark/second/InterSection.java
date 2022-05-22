package com.poype.bigdata.spark.second;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class InterSection {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        Integer[] numArray1 = {1, 2, 3, 4, 5};
        Integer[] numArray2 = {3, 4, 5, 6, 7};

        JavaRDD<Integer> dataRdd1 = sc.parallelize(Arrays.asList(numArray1));
        JavaRDD<Integer> dataRdd2 = sc.parallelize(Arrays.asList(numArray2));

        // 求两个RDD的交集
        JavaRDD<Integer> dataRdd3 = dataRdd1.intersection(dataRdd2);

        // [3, 4, 5]
        System.out.println(dataRdd3.collect());
    }
}
