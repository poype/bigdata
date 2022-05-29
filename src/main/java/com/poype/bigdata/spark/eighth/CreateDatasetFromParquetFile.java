package com.poype.bigdata.spark.eighth;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDatasetFromParquetFile {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("test")
                .master("local[*]")
                .getOrCreate();

        // parquet文件的加载非常简单
        Dataset<Row> dataset = sparkSession.read().format("parquet")
                .load("./src/main/resources/sql/users.parquet");

        // +------+--------------+----------------+
        // |  name|favorite_color|favorite_numbers|
        // +------+--------------+----------------+
        // |Alyssa|          null|  [3, 9, 15, 20]|
        // |   Ben|           red|              []|
        // +------+--------------+----------------+
        dataset.show();
    }
}
