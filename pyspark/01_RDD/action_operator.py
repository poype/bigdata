from pyspark import SparkContext, SparkConf
from typing import Iterable

# 计算相同的key有多少个
def test_count_by_key(sc: SparkContext) -> None:
    rdd = sc.parallelize([('a', 1), ('b', 1), ('c', 1), ('a', 11)], 3)

    print(rdd.countByKey())


# 将RDD各个分区内的数据统一收集到Driver中，形成一个List对象
def test_collect(sc: SparkContext) -> None:
    rdd = sc.parallelize([1, 2, 3, 4], 3)
    print(rdd.collect())


def test_reduce(sc: SparkContext) -> None:
    rdd = sc.parallelize([1, 2, 3, 4, 5], 3)
    # 15
    print(rdd.reduce(lambda a, b: a + b))


# 与reduce类似，但具有初始值
def test_fold(sc: SparkContext) -> None:
    rdd = sc.parallelize([1, 2, 3, 4, 5], 3)
    # 415, 各个分区内有初始值，分区间聚合的时候还有初始值
    print(rdd.fold(100, lambda a, b: a + b))


def test_first(sc: SparkContext) -> None:
    rdd = sc.parallelize([1, 2, 3, 4, 5], 3)
    # 1
    print(rdd.first())


def test_take(sc: SparkContext) -> None:
    rdd = sc.parallelize([1, 2, 3, 4, 5], 3)
    # [1, 2, 3] , 取前三个元素
    print(rdd.take(3))


def test_top(sc: SparkContext) -> None:
    rdd = sc.parallelize([111, 22, 63, 4, 25], 3)
    # [111, 63, 25]，取最大的三个元素
    print(rdd.top(3))


def test_count(sc: SparkContext) -> None:
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6], 3)
    # 6，6个元素
    print(rdd.count())

# 随机抽样RDD的数据，每次的结果是随机的，如果提供了seed，则每次取的数是一样的
def test_sample(sc: SparkContext) -> None:
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)
    print(rdd.takeSample(False, 3))
    # 如果允许可以重复，则采样数可以大于RDD中元素的数量
    print(rdd.takeSample(True, 100))

    # 每次的结果是相同的 [7, 9, 10]
    print(rdd.takeSample(False, 3, 1))


def test_take_ordered(sc: SparkContext) -> None:
    rdd = sc.parallelize([11, 22, 13, 44, 25, 6, 27, 58, 19, 10], 3)
    # [6, 10, 11]，升序排列前三个
    print(rdd.takeOrdered(3))
    # 倒序排列前三个，[58, 44, 27]
    print(rdd.takeOrdered(3, lambda x: -x))


def test_foreach(sc: SparkContext) -> None:
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)

    # foreach什么都不返回
    # foreach是在Executor上直接执行的
    rdd.foreach(lambda x: print(x * 10))


def test_save_as_text_file(sc: SparkContext) -> None:
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)
    # 每个Executor会将自己的数据直接写入到HDFS，所以几个分区就会在HDFS上写几个文件
    rdd.saveAsTextFile("hdfs://node1:8020/input/num.txt")


# mapPartitions性能可能会比map好很多， 类似的还有forEachPartition
def test_map_partitions(sc: SparkContext) -> None:
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)
    result_rdd = rdd.mapPartitions(mul_10)
    print(result_rdd.collect())

def mul_10(num_collection: Iterable[int]) -> Iterable[int]:
    result = list()
    for it in num_collection:
        result.append(it * 10)
    return result

# 自定义分区逻辑
def test_partition_by(sc: SparkContext) -> None:
    rdd = sc.parallelize([(1, 'a'), (2, 'b'), (3, 'c'), (4, 'c'), (5, 'd'), (1, 'aa')], 3)
    # [[(1, 'a'), (2, 'b')], [(3, 'c'), (4, 'c')], [(5, 'd'), (1, 'aa')]]
    print(rdd.glom().collect())

    another_rdd = rdd.partitionBy(5, lambda num: num % 5)
    # [[(5, 'd')], [(1, 'a'), (1, 'aa')], [(2, 'b')], [(3, 'c')], [(4, 'c')]]
    print(another_rdd.glom().collect())


# 修改分区的数量，最好别用这个算子
def test_repartition(sc: SparkContext) -> None:
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)
    # [[1, 2, 3], [4, 5, 6], [7, 8, 9, 10]]
    print(rdd.glom().collect())

    rdd2 = rdd.repartition(2)
    # [[7, 8, 9, 10], [1, 2, 3, 4, 5, 6]]
    print(rdd2.glom().collect())

if __name__ == "__main__":
    conf = SparkConf().setAppName("action").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # test_count_by_key(sc)

    # test_collect(sc)

    # test_reduce(sc)

    # test_fold(sc)

    # test_first(sc)

    # test_take(sc)

    # test_top(sc)

    # test_count(sc)

    # test_sample(sc)

    # test_take_ordered(sc)

    # test_foreach(sc)

    # test_save_as_text_file(sc)

    # test_map_partitions(sc)

    # test_partition_by(sc)

    test_repartition(sc)