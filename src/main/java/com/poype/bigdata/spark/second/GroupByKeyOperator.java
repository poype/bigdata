package com.poype.bigdata.spark.second;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class GroupByKeyOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> fruitList = new ArrayList<>();
        fruitList.add(new Tuple2<>("apple", 1));
        fruitList.add(new Tuple2<>("orange", 2));
        fruitList.add(new Tuple2<>("banana", 3));
        fruitList.add(new Tuple2<>("orange", 1));
        fruitList.add(new Tuple2<>("orange", 1));
        fruitList.add(new Tuple2<>("apple", 4));
        JavaPairRDD<String, Integer> fruitRdd = sc.parallelizePairs(fruitList);

        JavaPairRDD<String, Iterable<Integer>> resultRdd = fruitRdd.groupByKey();
        // [(apple,[1, 4]), (banana,[3]), (orange,[2, 1, 1])]
        System.out.println(resultRdd.collect());
    }
}
