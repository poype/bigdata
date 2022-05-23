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

public class SortByOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        Integer[] numArray = {1, 8, 9, 7, 2, 3, 4, 5, 6};

        JavaRDD<Integer> dataRdd = sc.parallelize(Arrays.asList(numArray), 3);

        // 第一个lambda参数的返回值表示根据元素的什么值进行排序，因为这里只是简单的Integer类型，所以就根据元素本身进行排序
        // 第二个boolean类型参数表示是升序还是降序，true是升序，false是降序
        // 第三个参数是排序的分区数，排序操作是在各个executor中发生的，如果要求全局有序，那么分区数就要设置成1。但在local模式体现不出来
        JavaRDD<Integer> sortedRdd =
                dataRdd.sortBy((Function<Integer, Integer>) num -> num, true, 3);

        System.out.println(sortedRdd.collect());

        List<Tuple2<String, Integer>> fruitList = new ArrayList<>();
        fruitList.add(new Tuple2<>("orange", 3));
        fruitList.add(new Tuple2<>("apple", 2));
        fruitList.add(new Tuple2<>("banana", 1));

        JavaPairRDD<String, Integer> fruitRdd = sc.parallelizePairs(fruitList);
        JavaPairRDD<String, Integer> sortedFruitRdd = fruitRdd.sortByKey();
        // [(apple,2), (banana,1), (orange,3)]
        System.out.println(sortedFruitRdd.collect());
    }
}
