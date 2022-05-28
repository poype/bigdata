package com.poype.bigdata.spark.eighth;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSessionStudy {

    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder()
                                                .appName("test")
                                                .master("local[*]")
                                                .getOrCreate();

        // 通过sparkSession对象可以获取到SparkContext对象
        SparkContext sc = sparkSession.sparkContext();

        Dataset<Row> dataset = sparkSession.read().csv("./src/main/resources/stu_score.txt");

        // 给每一列指定名称，如果不指定，则列的名称为|_c0| _c1|_c2|
        Dataset<Row> schemaDataset = dataset.toDF("id", "name", "score");

        // |-- id: string (nullable = true)
        // |-- name: string (nullable = true)
        // |-- score: string (nullable = true)
        schemaDataset.printSchema();

        // only showing top 20 rows
        schemaDataset.show();

        // score相当于是表名
        schemaDataset.createTempView("score");

        // | id|name|score|
        // +---+----+-----+
        // |  1|数学|   96|
        // |  2|数学|   96|
        // |  3|数学|   96|
        // |  4|数学|   96|
        // |  5|数学|   96|
        sparkSession.sql("SELECT * FROM score WHERE name = '数学' LIMIT 5").show();
    }
}
