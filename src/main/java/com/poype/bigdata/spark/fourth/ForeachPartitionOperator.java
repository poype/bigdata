package com.poype.bigdata.spark.fourth;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;

public class ForeachPartitionOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Integer[] nums = {3, 4, 1, 2, 6, 5, 10, 8, 7, 9};

        JavaRDD<Integer> dataRdd = sc.parallelize(Arrays.asList(nums), 3);

        // 一次处理一整个分区
        dataRdd.foreachPartition((VoidFunction<Iterator<Integer>>) numIterator -> {
            StringBuilder sb = new StringBuilder("");
            while(numIterator.hasNext()) {
                sb.append(numIterator.next()).append(" ");
            }
            System.out.println(sb);
        });
        // 三个分区，分别打印如下结果
        // 2 6 5
        // 10 8 7 9
        // 3 4 1
    }
}
