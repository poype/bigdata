package com.poype.bigdata.spark.fifth;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RddHandleProcess {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Integer[] nums = {3, 4, 1, 2, 6, 5, 10, 8, 7, 9};

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(nums), 1);

        // rdd2会被构建两次
        JavaRDD<Integer> rdd2 = rdd1.mapPartitions((FlatMapFunction<Iterator<Integer>, Integer>) numIterator -> {
            System.out.println("-----------rdd2-----------");
            List<Integer> list = new ArrayList<>();
            while(numIterator.hasNext()) {
                list.add(numIterator.next() * 10);
            }
            return list.iterator();
        });

        JavaRDD<Integer> rdd3 = rdd2.filter((Function<Integer, Boolean>) v1 -> {
            System.out.println("----------rdd3--------------");
            return v1 > 50;
        });

        JavaRDD<Integer> rdd4 = rdd2.filter((Function<Integer, Boolean>) v1 -> {
            System.out.println("----------rdd4--------------");
            return v1 <= 50;
        });

        System.out.println(rdd3.collect());
        System.out.println(rdd4.collect());
    }
}
