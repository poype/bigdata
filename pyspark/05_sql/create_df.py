from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

# 先获取的DataFrame对象，然后创建 temp view 之后就可以使用sql了。

# 数据来自于RDD
def df_with_rdd(spark: SparkSession):
    sc = spark.sparkContext

    rdd = (sc.textFile("../data/people.txt")
           .map(lambda line: line.split(","))
           .map(lambda item: (item[0], int(item[1]))))

    df = spark.createDataFrame(rdd, schema=["name", "age"])

    # 打印表结构
    df.printSchema()

    df.show(10, False)

    df.createOrReplaceTempView("people")
    spark.sql("select * from people where age = 30").show()


def test_struct_type(spark: SparkSession):
    sc = spark.sparkContext

    rdd = (sc.textFile("../data/people.txt")
           .map(lambda line: line.split(","))
           .map(lambda item: (item[0], int(item[1]))))

    schema = (StructType().add("name", StringType(), nullable=True)
                        .add("age", IntegerType(), nullable=True))

    df = spark.createDataFrame(rdd, schema=schema)
    df.printSchema()
    df.show(10, False)


def test_struct_type2(spark: SparkSession):
    sc = spark.sparkContext

    rdd = (sc.textFile("../data/people.txt")
           .map(lambda line: line.split(","))
           .map(lambda item: (item[0], int(item[1]))))

    schema = (StructType().add("name", StringType(), nullable=True)
              .add("age", IntegerType(), nullable=True))

    df = rdd.toDF(schema=schema)

    df.printSchema()
    df.show(10, False)

    df.createOrReplaceTempView("people")
    spark.sql("select * from people where age = 19").show()


def test_read_json(spark: SparkSession):
    # json数据类型自带schema
    df = spark.read.format("json").load("../data/people.json")

    df.printSchema()
    df.show(10, False)


def test_read_csv(spark: SparkSession):
    df = (spark.read.format("csv")
          .option("sep", ";")
          .option("encoding", "utf-8")
          .option("header", "true")
          .schema("name STRING, age INT, job STRING")
          .load("../data/people.csv"))

    df.show(100, False)

    df.createOrReplaceTempView("people")
    spark.sql("select * from people where age = 11").show()


def test_read_parquet(spark: SparkSession):
    # parquet 自带 schema
    df = spark.read.format("parquet").load("../data/users.parquet")
    df.printSchema()
    df.show(100, False)


def test_dsl(spark: SparkSession):
    df = (spark.read.format("csv")
          .option("sep", ",")
          .schema("id INT, subject STRING, score INT")
          .load("../data/stu_score.txt"))

    # df.show(100, False)

    id_column = df['id']
    subject_column = df['subject']

    # 有下面三种方式查询对应的列
    df.select(["id", "subject"]).show()
    df.select("id", "subject").show()
    df.select(id_column, subject_column).show()

    # filter 和 where 是一样的
    df.filter("score < 99 AND subject = '数学' AND id < 5").show()
    df.where("score < 99 AND subject = '数学' AND id < 5").show()

    df.groupby("subject").count().show()
    df.groupby("subject").sum("score").show()


def test_create_view(spark: SparkSession):
    df = (spark.read.format("csv")
          .option("sep", ",")
          .schema("id INT, subject STRING, score INT")
          .load("../data/stu_score.txt"))

    df.createOrReplaceTempView("scores1") # 注册一个临时表，如果存在就替换
    df.createTempView("scores2") # 注册一个临时表
    df.createGlobalTempView("scores3") # 注册一个全局表，全局表可以跨越多个SparkSession对象使用，查询前带上前缀“global_temp.”，用的应该不多

    spark.sql("select * from scores1 where subject= '语文' limit 2").show()
    spark.sql("select * from scores2 where subject= '数学' limit 2").show()
    spark.sql("select * from global_temp.scores3 where subject= '英语' limit 2").show()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

    # df_with_rdd(spark)

    # test_struct_type(spark)

    # test_struct_type2(spark)

    # test_read_json(spark)

    # test_read_csv(spark)

    # test_read_parquet(spark)

    # test_dsl(spark)

    test_create_view(spark)