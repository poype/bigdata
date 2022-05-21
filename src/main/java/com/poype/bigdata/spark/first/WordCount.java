package com.poype.bigdata.spark.first;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String filePath = "D:\\test.txt";

        // 设置partition的数量是3
        JavaRDD<String> fileRdd = sc.textFile(filePath, 3);

        JavaRDD<String> wordRdd = fileRdd.flatMap((FlatMapFunction<String, String>) line -> {
            String[] wordArray = line.split(" ");
            return Arrays.asList(wordArray).iterator();
        });

        // 把 word 转换成 (word ,1)
        JavaPairRDD<String, Integer> wordNumPairRdd =
                wordRdd.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));

        // 聚合结果
        JavaPairRDD<String, Integer> resultRdd =
                wordNumPairRdd.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);

        System.out.println(resultRdd.collect());
    }
}





