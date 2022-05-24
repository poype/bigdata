package com.poype.bigdata.spark.fourth;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MapPartitionsOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Integer[] nums = {3, 4, 1, 2, 6, 5, 10, 8, 7, 9};

        JavaRDD<Integer> dataRdd = sc.parallelize(Arrays.asList(nums), 3);

        // 把RDD中的每个元素都乘以10，与map算子非常类似，但操作过程是以partition为单位批量进行的
        // map算子一次操作一条记录，mapPartitions算子一次操作一个partition
        JavaRDD<Integer> resultRdd = dataRdd.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> numIterator) throws Exception {
                List<Integer> list = new ArrayList<>();
                while (numIterator.hasNext()) {
                    list.add(numIterator.next() * 10);
                }
                return list.iterator();
            }
        });
        System.out.println(resultRdd.collect());
    }
}
// 相比与map算子，mapPartitions算子性能更高