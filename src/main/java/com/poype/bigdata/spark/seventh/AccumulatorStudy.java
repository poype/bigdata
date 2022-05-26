package com.poype.bigdata.spark.seventh;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;
import java.util.Arrays;

public class AccumulatorStudy {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Integer[] numArray = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        JavaRDD<Integer> dataRdd = sc.parallelize(Arrays.asList(numArray), 3);

//        final Accumulator counter = new Accumulator();

        // 构建一个Spark的Accumulator
        final LongAccumulator counter = sc.sc().longAccumulator();

        dataRdd.foreach(integer -> {
            counter.add(1L);
            System.out.println(counter.value());
        });

        // RDD中一共有10个数字，期待总数是10，但却是0。原因是累加的操作是在Executor执行的，而下面的打印是在Driver中执行的
        // counter value: 0
        // 换用Spark的Accumulator后，打印结果是10
        System.out.println("counter value: " + counter.value());
    }
}

// Accumulator对象要从Driver传给Executor，必须要支持序列化
class Accumulator implements Serializable {

    private int value;

    public void addOne() {
        value++;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Accumulator{" +
                "value=" + value +
                '}';
    }
}
