package com.poype.bigdata.spark.second;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class UnionOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        Integer[] numArray1 = {1, 2, 3, 4, 5};
        Integer[] numArray2 = {6, 7, 8, 9};

        JavaRDD<Integer> dataRdd1 = sc.parallelize(Arrays.asList(numArray1));
        JavaRDD<Integer> dataRdd2 = sc.parallelize(Arrays.asList(numArray2));

        // union算子合并两个RDD
        JavaRDD<Integer> dataRdd3 = dataRdd1.union(dataRdd2);

        System.out.println(dataRdd3.collect());
    }
}
