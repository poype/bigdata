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


/**
 * collect算子也属于action类型
 * 将RDD各个partition的数据，统一收集到Driver中，形成一个list对象
 * 这个算子，是将RDD中各个分区数据都拉取到Driver，数据量可能很大
 * 所以用这个算子之前要对数据量大小心知肚明，结果数据集不会太大，
 * 不然，会把Driver内存撑爆
 */