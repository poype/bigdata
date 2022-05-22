package com.poype.bigdata.spark.second;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;

public class CreateRddFromLocalCollection {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Integer[] numArray = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21};

        // 将本地集合对象转换成分布式RDD，设置分片数是3
        JavaRDD<Integer> dataRdd = sc.parallelize(Arrays.asList(numArray), 3);

        System.out.println("分区数：" + dataRdd.getNumPartitions());

        // 查看所有数字是如何在各个partition之间分配的
        dataRdd.foreachPartition((VoidFunction<Iterator<Integer>>) nums -> {
            while (nums.hasNext()) {
                Integer num = nums.next();
                System.out.println(num);
            }
            System.out.println("---------------------------------");
        });

        // collect把每个partition中的数据都发送到Driver并构成一个本地集合List对象，即将分布式RDD转换成本地集合对象
        System.out.println(dataRdd.collect());
    }
}

/**
 * Spark RDD 编程的程序入口对象是SparkContext对象(不论何种编程语言)
 * 只有构建出SparkContext, 基于它才能执行后续的API调用和计算
 * 本质上, SparkContext对编程来说, 主要功能就是创建第一个RDD出来
 *
 * RDD的创建主要有2种方式:
 * 通过并行化集合创建( 将本地集合对象转换成分布式RDD )
 * 读取外部数据源( 读取文件数据创建分布式RDD )
 */
