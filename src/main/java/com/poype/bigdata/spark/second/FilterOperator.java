package com.poype.bigdata.spark.second;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class FilterOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        Integer[] numArray = {1, 2, 3, 4, 5, 6, 7, 8, 9};

        JavaRDD<Integer> dataRdd = sc.parallelize(Arrays.asList(numArray));

        // filter算子的返回值一定是boolean类型，返回值是true的保留
        JavaRDD<Integer> resultRdd = dataRdd.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer num) throws Exception {
                return num % 2 == 0;
            }
        });
        // [2, 4, 6, 8]
        System.out.println(resultRdd.collect());
    }
}
