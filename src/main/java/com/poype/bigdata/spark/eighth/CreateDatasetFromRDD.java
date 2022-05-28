package com.poype.bigdata.spark.eighth;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDatasetFromRDD {

    public static void main(String[] args) throws AnalysisException {

        SparkSession sparkSession = SparkSession.builder()
                                                .appName("test")
                                                .master("local[*]")
                                                .getOrCreate();

        // SparkSession对象转SparkContext对象
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        JavaRDD<String> fileRdd = sc.textFile("./src/main/resources/sql/people.txt", 3);
        JavaRDD<Student> studentRdd = fileRdd.map(line -> {
            String[] lineArray = line.split(",");
            return new Student(lineArray[0], lineArray[1]);
        });

        Dataset<Row> dataset = sparkSession.createDataFrame(studentRdd, Student.class);

        // +---+-------+
        // |age|   name|
        // +---+-------+
        // | 29|Michael|
        // | 30|   Andy|
        // | 19| Justin|
        // +---+-------+
        dataset.show();

        // root
        // |-- age: string (nullable = true)
        // |-- name: string (nullable = true)
        dataset.printSchema();

        dataset.createTempView("student");

        sparkSession.sql("select * from student where age < 20").show();

        // 第一个参数表示显示的行数，默认20
        // 第二个参数表示是否对列进行截断，如果一个列超过20个字符，后续的内容不显示
        dataset.show(2, false);
        // +---+-------+
        // |age|name   |
        // +---+-------+
        // | 29|Michael|
        // | 30|Andy   |
        // +---+-------+
        // only showing top 2 rows
    }
}


