package com.poype.bigdata.spark.second;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GroupByOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        Integer[] numArray = {1, 2, 3, 4, 5, 6, 7, 8, 9};

        JavaRDD<Integer> dataRdd = sc.parallelize(Arrays.asList(numArray));
        JavaPairRDD<Integer, Iterable<Integer>> groupRdd = dataRdd.groupBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v) throws Exception {
                return v % 2;
            }
        });

        // [(0,[2, 4, 6, 8]), (1,[1, 3, 5, 7, 9])]
        System.out.println(groupRdd.collect());

        List<Tuple2<String, Integer>> fruitList = new ArrayList<>();
        fruitList.add(new Tuple2<>("apple", 1));
        fruitList.add(new Tuple2<>("orange", 2));
        fruitList.add(new Tuple2<>("banana", 3));
        fruitList.add(new Tuple2<>("orange", 1));
        fruitList.add(new Tuple2<>("orange", 1));
        fruitList.add(new Tuple2<>("apple", 4));
        JavaPairRDD<String, Integer> fruitRdd = sc.parallelizePairs(fruitList);

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> fruitGroupRdd =
            fruitRdd.groupBy(new Function<Tuple2<String, Integer>, String>() {
                @Override
                public String call(Tuple2<String, Integer> fruit) throws Exception {
                    return fruit._1;
                }
            });
        // 按照fruit名字分成两个group
        // [(apple,[(apple,1), (apple,4)]), (banana,[(banana,3)]), (orange,[(orange,2), (orange,1), (orange,1)])]
        System.out.println(fruitGroupRdd.collect());
    }
}

// groupBy算子的返回值作为分组的key，可以支持非常灵活的分组算法
