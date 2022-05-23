package com.poype.bigdata.spark.second;

import org.apache.hadoop.shaded.org.eclipse.jetty.util.ajax.JSON;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class OperatorDemo {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> fileRdd = sc.textFile("./src/main/resources/order.txt");

        JavaRDD<String> jsonRdd =
                fileRdd.flatMap((FlatMapFunction<String, String>)
                        line -> Arrays.asList(line.split(" ")).iterator());

        // 提取城市名称和category两个字段
        JavaPairRDD<String, String> cityCategoryRdd =
                jsonRdd.mapToPair((PairFunction<String, String, String>) json -> {
            Map<String, String> jsonObj = (Map<String, String>) JSON.parse(json);
            return new Tuple2<>(jsonObj.get("areaName"), jsonObj.get("category"));
        });

        // 根据城市名称进行过滤，只保留北京的记录
        JavaPairRDD<String, String> filterCityCategoryRdd =
                cityCategoryRdd.filter((Function<Tuple2<String, String>, Boolean>)
                        cityCategory -> cityCategory._1.equals("北京"));

        // 去重
        JavaPairRDD<String, String> distinctCityCategoryRdd = filterCityCategoryRdd.distinct(1);

        // [(北京,书籍), (北京,服饰), (北京,家具), (北京,电脑), (北京,食品), (北京,家电), (北京,平板电脑), (北京,手机)]
        System.out.println(distinctCityCategoryRdd.collect());
    }
}

/**
 * 这是对前面学习的一个总结，根据order.txt文件中的订单数据，求北京这个地区都包含哪些category，且不能重复
 */
