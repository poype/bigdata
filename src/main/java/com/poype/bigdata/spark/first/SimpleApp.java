package com.poype.bigdata.spark.first;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SimpleApp {

    public static void main(String[] args) {
        String logFile = "D:\\spark\\spark\\README.md";
        SparkSession spark = SparkSession.builder()
                .appName("Simple Application")
                .master("local[*]")
                .getOrCreate();


        Dataset<String> logData = spark.read().textFile(logFile).cache();

        System.out.println("poype start:------------------------");
        System.out.println(logData);
        System.out.println("poype end:--------------------------");

        spark.stop();
    }
}
