package com.poype.bigdata.spark.second;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FlatMapOperator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        List<List<Integer>> sourceData = new ArrayList<>();
        sourceData.add(Arrays.asList(1, 2, 3));
        sourceData.add(Arrays.asList(4, 5, 6));
        sourceData.add(Arrays.asList(7, 8, 9));

        JavaRDD<List<Integer>> dataRdd = sc.parallelize(sourceData);

        // 这里我特意没有用lambda表达式
        JavaRDD<Integer> resultRdd1 = dataRdd.flatMap(new FlatMapFunction<List<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(List<Integer> list) throws Exception {
                return list.iterator();
            }
        });

        System.out.println(resultRdd1.collect());

        JavaRDD<Integer> resultRdd2 =
                dataRdd.flatMap((FlatMapFunction<List<Integer>, Integer>) List::iterator);
        System.out.println(resultRdd2.collect());

        List<String> strList = Arrays.asList("aaa bbb ccc", "ddd eee fff", "apple orange china korean");
        JavaRDD<String> lineRdd = sc.parallelize(strList, 3);

        JavaRDD<String> wordRdd = lineRdd.flatMap((FlatMapFunction<String, String>) s -> {
            List<String> wordList = Arrays.asList(s.split(" "));
            return wordList.iterator();
        });
        System.out.println(wordRdd.collect());
    }
}

/**
 * flatMap算子的功能是先对各个元素执行map操作，再对元素进行解除嵌套。
 * 例如先对lineRdd中的每一行执行split操作，再对word集合解除嵌套
 */
