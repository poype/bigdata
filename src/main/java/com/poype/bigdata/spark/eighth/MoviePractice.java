package com.poype.bigdata.spark.eighth;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.TimeUnit;

public class MoviePractice {

    public static void main(String[] args) throws AnalysisException, InterruptedException {
        SparkSession sparkSession = SparkSession.builder()
                .appName("test")
                .master("local[*]")
                .getOrCreate();

        // SparkSession对象转SparkContext对象
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        JavaRDD<String> fileRdd = sc.textFile("./src/main/resources/sql/u.data", 1);

        JavaRDD<MovieScore> movieScoreRDD = fileRdd.map(line -> {
            int startIndex = 0;
            int wordCount = 0;

            String[] lineArray = new String[4];

            for(int i = 0; i < line.length(); i++) {
                if (line.charAt(i) == ' ' || line.charAt(i) == '\t') {
                    lineArray[wordCount] = line.substring(startIndex, i);
                    wordCount++;

                    startIndex = i + 1;
                    while(line.charAt(startIndex) == ' ' || line.charAt(startIndex) == '\t') {
                        startIndex++;
                    }
                    i = startIndex;
                }
            }
            lineArray[3] = line.substring(startIndex);
            return new MovieScore(lineArray[0], lineArray[1], Integer.parseInt(lineArray[2]), lineArray[3]);
        });

        Dataset<Row> dataset = sparkSession.createDataFrame(movieScoreRDD, MovieScore.class);

        dataset.cache();

        dataset.show();

        dataset.createTempView("movie_score");

        // 用户平均分
        // +------+------------------+
        // |userId|         avg_score|
        // +------+------------------+
        // |   181|1.4919540229885058|
        // |   405|1.8344640434192674|
        // |   445|1.9851851851851852|
        // |   685|              2.05|
        // |   774|2.0580357142857144|
        // |   724| 2.164705882352941|
        // |   206|          2.171875|
        // |   660|2.5848214285714284|
        // |   824|               2.6|
        // +------+------------------+
        dataset.groupBy("userId")
                .avg("score")
                .withColumnRenamed("avg(score)", "avg_score") // 给列起一个更友好的名字
                .orderBy("avg_score")
                .show();

        // 电影平均分, 还是用SQL更灵活
        //  +-------+---------+
        //|movieId|avg_score|
        //+-------+---------+
        //|   1201|      5.0|
        //|   1293|      5.0|
        //|   1599|      5.0|
        //|    169|     4.47|
        //|    483|     4.46|
        //|     64|     4.45|
        //+-------+---------+
        sparkSession.sql("SELECT movieId, ROUND(AVG(score), 2) as avg_score FROM movie_score " +
                "GROUP BY movieId " +
                "ORDER BY avg_score DESC").show();

        // 求大于平均分数的那些movie记录
        // 获取平均值的过程比较麻烦
//        Dataset<Row> avgDataset = sparkSession.sql("SELECT AVG(score) FROM movie_score");
//        JavaRDD<Row> avgRdd = avgDataset.javaRDD();
//        double avgValue = avgRdd.map(row -> row.getDouble(0)).first();
//
//        sparkSession.sql("SELECT * FROM movie_score where score > " + avgValue).show();
//
        sparkSession.sql("SELECT * FROM movie_score WHERE score > (SELECT AVG(score) FROM movie_score)").show();

        TimeUnit.MILLISECONDS.sleep(999999);
    }
}
