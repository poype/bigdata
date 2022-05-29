package com.poype.bigdata.spark.eighth;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDatasetFromJsonFile {

    public static void main(String[] args) throws AnalysisException {
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

        // 执行SQL需要先创建临时试图
        dataset.createTempView("people");
//        dataset.createOrReplaceTempView("people");
//        dataset.createGlobalTempView("people");

        // +---+------+
        // |age|  name|
        // +---+------+
        // | 19|Justin|
        // +---+------+
        sparkSession.sql("select * from people where age < 20").show();
    }
}
