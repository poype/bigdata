from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("WordCount").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    rdd = sc.textFile("../data/words.txt").flatMap(lambda line: line.split(' ')).map(lambda word: [word])

    df = rdd.toDF(["word"])
    df.createTempView("words")

    spark.sql("SELECT word, COUNT(*) as count FROM words group by word").show()

    # +------+-----+
    # |  word|count|
    # +------+-----+
    # | hello|    3|
    # | spark|    1|
    # |hadoop|    1|
    # | flink|    1|
    # +------+-----+
