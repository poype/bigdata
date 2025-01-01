from pyspark.sql import SparkSession

# SparkSession既可以作为SparkSql的编程入口，
# 也可以从其获取到SparkContext对象作为RDD的编程入口

if __name__ == "__main__":
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

    sc = spark.sparkContext

    # SparkSql的HelloWord
    # DataFrame
    df = spark.read.csv("../data/stu_score.txt", sep=',', header=False)
    df2 = df.toDF("id", "name", "score")
    df2.printSchema()
    df2.show()

    df2.createTempView("score")

    # SQL 风格
    spark.sql("""
        SELECT * FROM score WHERE name = '英语' LIMIT 5
    """).show()

    # DSL 风格
    df2.where("name='数学'").limit(5).show()
