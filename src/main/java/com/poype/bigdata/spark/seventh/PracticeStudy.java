package com.poype.bigdata.spark.seventh;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class PracticeStudy {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Set<String> specialCharSet = new HashSet<>();
        specialCharSet.add(",");
        specialCharSet.add(".");
        specialCharSet.add("!");
        specialCharSet.add("#");
        specialCharSet.add("$");
        specialCharSet.add("%");

        Broadcast<Set<String>> specialCharSetBroadcast = sc.broadcast(specialCharSet);

        JavaRDD<String> fileRdd = sc.textFile("./src/main/resources/accumulator_broadcast_data.txt", 3);

        JavaRDD<String> wordRdd = fileRdd.flatMap(line -> {
            String[] lineArray = line.split(" ");
            return Arrays.asList(lineArray).iterator();
        });

        wordRdd.cache();

        // 过滤出正常的词
        JavaRDD<String> normalWordRdd = wordRdd.filter(word -> {
            if (word.length() == 0) {
                return false;
            }
            if (specialCharSetBroadcast.value().contains(word)) {
                return false;
            }
            return true;
        });

        JavaPairRDD<String, Integer> normalWordMapRdd = normalWordRdd.mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> normalWordCountRdd = normalWordMapRdd.reduceByKey(Integer::sum);

        // [(sql,2), (hadoop,3), (hdfs,2), (spark,11), (hive,6), (mapreduce,4)]
        System.out.println(normalWordCountRdd.collect());

        JavaRDD<String> specialCharRdd = wordRdd.filter(specialCharSetBroadcast.value()::contains);

        // 特殊字符的个数：8
        System.out.println("特殊字符的个数：" + specialCharRdd.count());
    }
}
