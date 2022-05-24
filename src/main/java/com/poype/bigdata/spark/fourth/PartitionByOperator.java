package com.poype.bigdata.spark.fourth;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PartitionByOperator {

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

        JavaPairRDD<String, Integer> wordListRdd = sc.parallelizePairs(wordList, 3);

        JavaPairRDD<String, Integer> partitionRdd = wordListRdd.partitionBy(new Partitioner() {
            // 分区总数
            @Override
            public int numPartitions() {
                return 3;
            }

            // 分区编号
            @Override
            public int getPartition(Object key) {
                if (key.equals("aaa")) {
                    return 0;
                } else if (key.equals("bbb")) {
                    return 1;
                } else {
                    return 2;
                }
            }
        });

        // 初始的分区状态
        // [[(aaa,1), (aaa,1)], [(bbb,1), (bbb,1), (bbb,1)], [(bbb,1), (ccc,1), (ccc,1)]]
        System.out.println(wordListRdd.glom().collect());

        // 自定义分区后的状态，所有的bbb都到了第1号分区
        // [[(aaa,1), (aaa,1)], [(bbb,1), (bbb,1), (bbb,1), (bbb,1)], [(ccc,1), (ccc,1)]]
        System.out.println(partitionRdd.glom().collect());

        // repartition调正分区数量为5，但分区规则不变
        // [[(bbb,1)], [(aaa,1)], [(aaa,1), (bbb,1)], [(bbb,1), (ccc,1)], [(bbb,1), (ccc,1)]]
        System.out.println(wordListRdd.repartition(5).glom().collect());
    }
}
