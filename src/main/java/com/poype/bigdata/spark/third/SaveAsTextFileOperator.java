package com.poype.bigdata.spark.third;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SaveAsTextFileOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Integer[] nums = {3, 4, 1, 2, 6, 5, 10, 8, 7, 9};

        JavaRDD<Integer> dataRdd = sc.parallelize(Arrays.asList(nums), 3);

        // 输出的文件跟hadoop一样，同样包含crc校验文件和_SUCCESS标志文件
        // 每个分区都对应一个part-xxx输出文件，每个元素输出一行
        dataRdd.saveAsTextFile("./test_save_as_text_file1");
    }
}
