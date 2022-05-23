package com.poype.bigdata.spark.third;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class TakeSampleOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Integer[] nums = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        JavaRDD<Integer> dataRdd = sc.parallelize(Arrays.asList(nums), 3);

        // takeSample有两个参数，第一个参数表示是否允许多次取相同位置上的元素，第二个参数是元素个数
        // 从RDD中随机取出5个元素，且同一个位置上的元素最多只能取一次
        List<Integer> sampleList = dataRdd.takeSample(false, 5, 99);
        // takeSample算子还能接受一个种子参数，如果不提供，那么它会使用一个随机数作为种子
        // 种子相同，每次取出的随机数就相同。所以通常不设置这个参数，让其使用随机的种子

        System.out.println(sampleList);
    }
}
