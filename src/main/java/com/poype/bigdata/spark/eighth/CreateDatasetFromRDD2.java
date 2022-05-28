package com.poype.bigdata.spark.eighth;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;

public class CreateDatasetFromRDD2 {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("test")
                .master("local[*]")
                .getOrCreate();

        // SparkSession对象转SparkContext对象
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        JavaRDD<String> fileRdd = sc.textFile("./src/main/resources/sql/people.txt", 3);
        // 把字符串映射成Row对象
        JavaRDD<Row> rowRdd = fileRdd.map(line -> {
            String[] lineArray = line.split(",");
            return RowFactory.create(lineArray[0], Integer.parseInt(lineArray[1].trim()));
        });

        // 构造schema
        List<StructField> fields = new ArrayList<>();

        StructField nameField = DataTypes.createStructField("name", DataTypes.StringType, true);
        StructField ageField = DataTypes.createStructField("age", DataTypes.IntegerType, true);
        fields.add(nameField);
        fields.add(ageField);

        StructType schema = DataTypes.createStructType(fields);

        // 构建Dataset
        Dataset<Row> dataset = sparkSession.createDataFrame(rowRdd, schema);

        dataset.printSchema();

        dataset.show();
    }
}
