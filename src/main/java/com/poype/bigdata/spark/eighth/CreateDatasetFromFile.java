package com.poype.bigdata.spark.eighth;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class CreateDatasetFromFile {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                                                .appName("test")
                                                .master("local[*]")
                                                .getOrCreate();

        // 构造schema
        List<StructField> fields = new ArrayList<>();
        StructField nameField = DataTypes.createStructField("name", DataTypes.StringType, true);
        StructField ageField = DataTypes.createStructField("age", DataTypes.StringType, true);
        fields.add(nameField);
        fields.add(ageField);
        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> dataset = sparkSession.read().format("csv").schema(schema)
                .load("./src/main/resources/sql/people.txt");

        // +-------+---+
        // |   name|age|
        // +-------+---+
        // |Michael| 29|
        // |   Andy| 30|
        // | Justin| 19|
        // +-------+---+
        dataset.show();
    }
}
