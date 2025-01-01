from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("MovieStatistics").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    schema = (StructType().add("user_id", StringType(), nullable=True)
              .add("movie_id", StringType(), nullable=True)
              .add("rank", IntegerType(), nullable=True)
              .add("timestamp", StringType(), nullable=True))

    df = (spark.read
          .format('csv')
          .option('header', False)
          .option('sep', '\t')
          .option('encoding', 'utf-8')
          .schema(schema)
          .load("../data/u.data"))

    df.createTempView("movie_rating")

    # 查询用户平均分，
    (df.groupby("user_id")
        .avg('rank')
        .withColumnRenamed("avg(rank)", "avg_rank")
        .withColumn("avg_rank", F.round("avg_rank", 2))
        .orderBy("avg_rank", ascending=False)
        .show())


    # 查询用户平均分
    # +-------+------------------+
    # |user_id|    average_rating|
    # +-------+------------------+
    # |    296|4.1768707482993195|
    # |    467|3.6818181818181817|
    # |    691|           4.21875|
    result_df = spark.sql("SELECT user_id, avg(rank) as average_rank FROM movie_rating group by user_id")

    # DF 其实也是RDD，RDD是Row对象的List
    # [Row(user_id='296', average_rank=4.1768707482993195)
    print(result_df.rdd.collect())

    # 电影的平均分查询
    spark.sql("SELECT movie_id, ROUND(avg(rank), 2) as average_rank FROM movie_rating group by movie_id ORDER BY average_rank DESC").show()

    avg_rank = df.select(F.avg(df['rank'])).first()['avg(rank)']

    # 查询大于平均分的电影的数量
    cnt = df.where(df['rank'] > avg_rank).count()
    print("大于平均分的电影的数量: ", cnt)

    # 查询高分电影中（>3）打分次数最多的用户，此人打分的平均分
    user_id_and_cnt_temp_df = spark.sql("SELECT user_id, count(*) as cnt FROM movie_rating WHERE rank > 3 group by user_id")
    user_id_and_cnt_temp_df.createTempView('user_id_and_cnt')

    user_id = spark.sql("SELECT user_id, max(cnt) FROM user_id_and_cnt GROUP BY user_id").first()['user_id']
    print("打分最多的user_id：", user_id)

    spark.sql(f'SELECT user_id, avg(rank) as avg_rank FROM movie_rating WHERE user_id = {user_id} GROUP BY user_id').show()