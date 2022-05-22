package com.poype.bigdata.spark.second;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 按照key进行分组，再按照value进行聚合
 * 在执行聚合时，是以叠加的方式对集合中的所有元素执行操作
 */
public class ReduceByKeyOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> fruitList = new ArrayList<>();
        fruitList.add(new Tuple2<>("apple", 2));
        fruitList.add(new Tuple2<>("orange", 3));
        fruitList.add(new Tuple2<>("banana", 1));
        fruitList.add(new Tuple2<>("apple", 3));
        fruitList.add(new Tuple2<>("banana", 6));
        fruitList.add(new Tuple2<>("apple", 1));
        fruitList.add(new Tuple2<>("orange", 1));

        JavaPairRDD<String, Integer> fruitRdd = sc.parallelizePairs(fruitList);

        // 泛型中，前两个类型是要聚合的两个value的类型，第三个类型是聚合的结果，它们的类型必须都相同
        JavaPairRDD<String, Integer> fruitCountRdd1 = fruitRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                // 这里并不只是两两相加，而是以累加的方式将相同key的所有元素的value相加在一起
                return v1 + v2;
            }
        });
        System.out.println(fruitCountRdd1.collect());

        // 使用lambda表达式就变得简洁多了
        JavaPairRDD<String, Integer> fruitCountRdd2 =
                fruitRdd.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);
        System.out.println(fruitCountRdd2.collect());
    }
}


