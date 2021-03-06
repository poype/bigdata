package com.poype.bigdata.spark.fifth;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RddHandleProcess {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Integer[] nums = {3, 4, 1, 2, 6, 5, 10, 8, 7, 9};

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(nums), 1);

        // rdd2会被构建两次
        JavaRDD<Integer> rdd2 = rdd1.mapPartitions((FlatMapFunction<Iterator<Integer>, Integer>) numIterator -> {
            System.out.println("-----------rdd2-----------");
            List<Integer> list = new ArrayList<>();
            while(numIterator.hasNext()) {
                list.add(numIterator.next() * 10);
            }
            return list.iterator();
        });

        // 将rdd2缓存到内存中，避免多次计算rdd2。现在rdd2只会被计算一次
        rdd2.persist(StorageLevel.MEMORY_ONLY());
        // rdd2.cache(); 这行代码也是将rdd缓存在内存，与上面代码相同

        // 依赖rdd2
        JavaRDD<Integer> rdd3 = rdd2.filter((Function<Integer, Boolean>) v1 -> {
            System.out.println("----------rdd3--------------");
            return v1 > 50;
        });

        // 依赖rdd2
        JavaRDD<Integer> rdd4 = rdd2.filter((Function<Integer, Boolean>) v1 -> {
            System.out.println("----------rdd4--------------");
            return v1 <= 50;
        });

        System.out.println(rdd3.collect());
        System.out.println(rdd4.collect());

        // 主动清理掉缓存
        rdd2.unpersist();
    }
}
/**
 * persist是把rdd缓存到executor所在服务器的内存或硬盘上，所以缓存数据是不安全的，有丢失的风险
 */