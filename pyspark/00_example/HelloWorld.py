from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    # local[*] 表示使用本地的所有资源
    conf = SparkConf().setAppName("WordCountHelloWorld")
    # 通过SparkConf对象构建SparkContext对象
    sc = SparkContext(conf=conf)

    # RDD[str]
    file_rdd = sc.textFile("hdfs://node1:8020/input/words.txt")

    # RDD[str]
    words_rdd = file_rdd.flatMap(lambda line: line.split(" "))

    # RDD[Tuple[str, int]]
    words_with_one_rdd = words_rdd.map(lambda word: (word, 1))

    # RDD[Tuple[str, int]]
    result_rdd = words_with_one_rdd.reduceByKey(lambda a, b: a + b)

    # List[Tuple[str, int]]
    final_result  = result_rdd.collect()

    print(final_result)


# SparkContext是Spark Application的程序入口，任何应用都需要先构建SparkContext对象。
# 构建SparkContext对象分两步：
# 1. 创建SparkConf对象。
# 2. 基于SparkConf对象，创建SparkContext对象。

# SparkContext对象会在Driver上构建好，通过序列化的方式分发给每个Executor。
# 接下来的RDD计算会在各个Executor上并发执行。
# collect方法会将各个Executor上的计算结果统一收集到Driver。
# Driver最后打印输入最终的计算结果。
# 大部分Spark应用最终都会将结果数据汇集到Driver。

# Spark RDD 编程的入口对象是SparkContext对象
