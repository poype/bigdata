package com.poype.bigdata.spark.eighth;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDatasetFromJsonFile {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("test")
                .master("local[*]")
                .getOrCreate();

        // 因为json格式自带schema信息，所以无需明确构建StructField对象
        Dataset<Row> dataset = sparkSession.read().format("json")
                .load("./src/main/resources/sql/people.json");

        // +---+-------+
        // |age|   name|
        // +---+-------+
        // | 22|Michael|
        // | 30|   Andy|
        // | 19| Justin|
        // +---+-------+
        dataset.show();
    }
}
