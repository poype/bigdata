package com.poype.bigdata.spark.second;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CreateRddFromFile {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // textFile第二个参数表示最小partition数量，但它的话语权不足，spark有自己的判断。
        // 例如你指定fileRdd有1000000个分区，那spark就会忽略这个参数
        // 一般情况下，我们不需要给textFile方法提供分区数参数
        JavaRDD<String> fileRdd = sc.textFile("D:\\test.txt", 3);
        // 文件内容以 “行(row)” 为单位存储

        System.out.println(fileRdd.collect());
    }
}
