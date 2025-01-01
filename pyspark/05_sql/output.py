from time import sleep

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("output").master("local[*]").getOrCreate()

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

    df.cache()

    # Write text,这种类型只能写出一个列的数据，所以这里先将多个column拼成一个column
    (df.select(F.concat_ws("---", "user_id", "movie_id", "rank", "timestamp"))
     .write
     .mode("overwrite")
     .format("text")
     .save("../data/output/movie_txt"))

    # Write csv, 这种模式直接就能写出多个column，默认用“,”分隔
    (df.write
     .mode("overwrite")
     .format("csv")
     .option("header", True)
     .save("../data/output/movie_csv"))

    # Write Json
    (df.write
     .mode("overwrite")
     .format("json")
     .save("../data/output/movie_json"))

    # Write parquet
    (df.write
     .mode("overwrite")
     .format("parquet")
     .save("../data/output/movie_parquet"))

    sleep(3600)
