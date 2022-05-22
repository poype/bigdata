package com.poype.bigdata.spark.second;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MapOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        Integer[] numArray = {1, 2, 3, 4, 5, 6, 7, 8, 9};

        JavaRDD<Integer> dataRdd = sc.parallelize(Arrays.asList(numArray));

        // 这里我特意不使用lambda表达式，Function中的两个泛型一个是入参，一个是结果
        JavaRDD<Integer> resultRdd1 = dataRdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer num) {
                return num * 10;
            }
        });

        // 使用lambda表达式后的简化版本
        JavaRDD<Integer> resultRdd2 = dataRdd.map((Function<Integer, Integer>) num -> num * 20);

        System.out.println("resultRdd1: " + resultRdd1.collect());
        System.out.println("resultRdd2: " + resultRdd2.collect());

        List<Tuple2<String, Integer>> fruitList = new ArrayList<>();
        fruitList.add(new Tuple2<>("apple", 1));
        fruitList.add(new Tuple2<>("orange", 2));
        fruitList.add(new Tuple2<>("banana", 3));

        JavaPairRDD<String, Integer> fruitRdd = sc.parallelizePairs(fruitList);

        // 利用mapToPair方法把所有水果的数量都乘以10，水果的名字保持不变
        JavaPairRDD<String, Integer> muchFruit1 =
            fruitRdd.mapToPair((PairFunction<Tuple2<String, Integer>, String, Integer>)
                originalFruit -> new Tuple2<>(originalFruit._1, originalFruit._2 * 10));
        System.out.println(muchFruit1.collect());

        // 因为需求只是要对每个pair中的value进行处理，所以使用mapValues方法更简单，因为mapValues只关注value，不在乎key
        // 下面把每个水果的数量都加10，可以看到相比于使用mapToPair方法代码简洁了很多
        JavaPairRDD<String, Integer> muchFruit2 =
                fruitRdd.mapValues((Function<Integer, Integer>) value -> value + 10);
        System.out.println(muchFruit2.collect());
    }
}
