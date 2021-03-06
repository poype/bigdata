package com.poype.bigdata.spark.third;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;

public class FoldOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Integer[] nums = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        JavaRDD<Integer> dataRdd = sc.parallelize(Arrays.asList(nums), 3);

        // 第一个参数是初始值，分区内聚合时有初始值，分区间聚合时也有初始值
        int sum = dataRdd.fold(10, (Function2<Integer, Integer, Integer>) Integer::sum);

        // 一共三个分区，每个分区内部都会额外增加10
        // 分区之间累加的时候也会再加一个10，所以总共额外增加了40

        // 95
        System.out.println(sum);

        // first算子取第一个元素
        System.out.println("first:" + dataRdd.first());

        // task 5: [1, 2, 3, 4, 5]  take算子取前5个元素
        System.out.println("take 5: " + dataRdd.take(5));

        // top 3: [10, 9, 8]  top算子从RDD中获取最大的3个元素
        System.out.println("top 3: " + dataRdd.top(3));

        // count: 10   count算子计算RDD中有多少条数据
        System.out.println("count: " + dataRdd.count());
    }
}
