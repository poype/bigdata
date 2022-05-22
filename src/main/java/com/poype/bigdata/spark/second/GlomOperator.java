package com.poype.bigdata.spark.second;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class GlomOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        Integer[] numArray = {1, 2, 3, 4, 5, 6, 7, 8, 9};

        JavaRDD<Integer> dataRdd1 = sc.parallelize(Arrays.asList(numArray), 3);

        // [1, 2, 3, 4, 5, 6, 7, 8, 9]
        System.out.println(dataRdd1.collect());

        // [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        System.out.println(dataRdd1.glom().collect());
    }
}
