package com.poype.bigdata.spark.third;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CountByKeyOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> wordList = new ArrayList<>();
        wordList.add(new Tuple2<>("aaa", 1));
        wordList.add(new Tuple2<>("aaa", 1));
        wordList.add(new Tuple2<>("bbb", 1));
        wordList.add(new Tuple2<>("bbb", 1));
        wordList.add(new Tuple2<>("bbb", 1));
        wordList.add(new Tuple2<>("bbb", 1));
        wordList.add(new Tuple2<>("ccc", 1));
        wordList.add(new Tuple2<>("ccc", 1));

        JavaPairRDD<String, Integer> wordListRdd = sc.parallelizePairs(wordList);

        // countByKey是action算子，它的返回值已经不是RDD了
        Map<String, Long> wordCount = wordListRdd.countByKey();
        System.out.println(wordCount);
    }
}
