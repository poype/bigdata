# 创建RDD的两种方式：
# 1. 本地集合 转 分布式RDD
# 2. 读取文件

from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("test_parallelize").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 第一个参数是本地集合，第二个参数的partition的数量
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)

    # 打印rdd分区的数量
    print(rdd.getNumPartitions())

    # 将RDD每个分区的数据都发送到Driver中，构成一个python list对象
    # 相当于将分布式RDD 转 本地集合
    print(rdd.collect())

    print("-------------------从文件创建rdd-------------------")

    file_rdd = sc.textFile("../00_example/note.txt")

    print(file_rdd.getNumPartitions())

    print(file_rdd.collect())

    print("-------------------wholeTextFile------------------")

    # wholeTextFiles 读取一个目录下的所有文件，读取小文件专用，对读取小文件做了优化。
    file_rdd_2 = sc.wholeTextFiles("../00_example/")

    print(file_rdd_2.collect())
