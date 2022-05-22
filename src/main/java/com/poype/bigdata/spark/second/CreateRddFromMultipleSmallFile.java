package com.poype.bigdata.spark.second;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class CreateRddFromMultipleSmallFile {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WordCount");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // wholeTextFiles适合读取一堆小文件，参数是所有小文件所在的目录
        // 这个方法会用少量的partition读取所有的文件，避免partition数量过多导致shuffle概率更高
        // fileRdd的key是文件名，value是文件的内容
        JavaPairRDD<String, String> fileRdd = sc.wholeTextFiles("./src/main/resources/tiny_files");

        System.out.println(fileRdd.collect());

        // 提取文件的内容
        JavaRDD<String> fileContent = fileRdd.map((Function<Tuple2<String, String>, String>) file -> file._2);

        System.out.println(fileContent.collect());
    }
}
