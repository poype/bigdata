package com.poype.bigdata.spark.eighth;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDatasetFromCsvFile {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("test")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = sparkSession.read()
                .format("csv")
                .option("sep", ";")                 // 指定分隔符
                .option("header", true)             // 指定文件中包含header
                .option("encoding", "utf-8")
                .schema("name STRING, age INT, job STRING")  // 通过字符串方式指定schema
                .load("./src/main/resources/sql/people.csv");

        // +-----+----+---------+
        // | name| age|      job|
        // +-----+----+---------+
        // |Jorge|  30|Developer|
        // |  Bob|  32|Developer|
        // |  Ani|  11|Developer|
        // | Lily|  11|  Manager|
        // |  Put|  11|Developer|
        // |Alice|   9|  Manager|
        // |Alice|   9|  Manager|
        // |Alice|   9|  Manager|
        // |Alice|   9|  Manager|
        // |Alice|null|  Manager|
        // |Alice|   9|     null|
        // +-----+----+---------+
        dataset.show();
    }
}
