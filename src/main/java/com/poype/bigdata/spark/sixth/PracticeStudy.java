package com.poype.bigdata.spark.sixth;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class PracticeStudy {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建文件RDD, 总共有6列，分别是 搜索时间、用户ID、搜索内容、URL返回排名、用户点击顺序、用户点击URL
        JavaRDD<String> fileRdd = sc.textFile("./src/main/resources/SogouQ.txt", 3);

        JavaRDD<List<String>> lineRdd = fileRdd.map(line -> {
            String[] lineArray = line.split("\t");
            return Arrays.asList(lineArray);
        });

        // 由于多次用到lineRdd，所以将它缓存
        lineRdd.persist(StorageLevel.MEMORY_ONLY());

        JavaPairRDD<String, Integer> searchWordRdd = lineRdd.mapToPair(line -> new Tuple2<>(line.get(2), 1));

        JavaPairRDD<String, Integer> wordCountRdd =
                searchWordRdd.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);

        // 这里必须明确指定SerializableComparator接口，否则会报”Task not serializable“错误
        // 原因可能是top算子会引入shuffle，所以要将对象在网络上传输。这里被传输的对象有两类：
        // 一类是Tuple2类型的对象，Tuple2类已经实现了Serializable接口，所以没问题
        // 另一类是实现Comparator接口的对象，这种对象虽然只是用于排序，但也会在网络上被传输，所以也要实现Serializable接口
        // 仔细想想Comparator对象应该没必要在网络上传输，root cause还需要深入研究一下。。。。。。。
        List<Tuple2<String, Integer>> top5World = wordCountRdd.top(5,
                (SerializableComparator<Tuple2<String, Integer>>) (o1, o2) -> o1._2 - o2._2);

        // [(scala,2310), (hadoop,2268), (博学谷,2002), (传智汇,1918), (itheima,1680)]
        System.out.println(top5World);

        // 求搜索的高峰时段，按小时计
        JavaPairRDD<String, Integer> hourRdd = lineRdd.mapToPair(line -> {
            String time = line.get(0);
            String hour = time.split(":")[0];
            Tuple2<String, Integer> hourTuple = new Tuple2<>(hour, 1);
            return hourTuple;
        });

        JavaPairRDD<String, Integer> hourCountRdd = hourRdd.reduceByKey(Integer::sum);

        // [(20,3479), (23,3087), (21,2989), (22,2499), (01,1365)]
        System.out.println(hourCountRdd.top(5,
                (SerializableComparator<Tuple2<String, Integer>>) (o1, o2) -> o1._2 - o2._2));
    }
}
