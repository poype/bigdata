from pyspark import SparkContext, SparkConf

# 测试map算子
def test_map(sc: SparkContext):
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)

    # 如果是只需一行的函数体，使用lambda更简单
    map_rdd_1 = rdd.map(lambda x: x * 10)
    # 对于多行的函数体，还是要定义独立的方法
    map_rdd_2 = rdd.map(add)

    # [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    print(map_rdd_1.collect())
    # [6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
    print(map_rdd_2.collect())

def add(x: int) -> int:
    return x + 5

# 先对rdd执行map操作，然后再执行解除嵌套的操作
def test_flat_map(sc: SparkContext):
    rdd = sc.parallelize(["hadoop spark hadoop", "spark spark hadoop", "hadoop spark spark"], 3)

    word_array_rdd = rdd.map(lambda line: line.split(" "))
    # [['hadoop', 'spark', 'hadoop'], ['spark', 'spark', 'hadoop'], ['hadoop', 'spark', 'spark']]
    print(word_array_rdd.collect())

    flat_word_array_rdd = rdd.flatMap(lambda line: line.split(" "))
    # ['hadoop', 'spark', 'hadoop', 'spark', 'spark', 'hadoop', 'hadoop', 'spark', 'spark']
    print(flat_word_array_rdd.collect())

# 针对KV型RDD，按照Key分区，然后根据所提供的聚合逻辑，完成组内数据的聚合操作
def test_reduce_by_key(sc: SparkContext) -> None:
    rdd = sc.parallelize([('a', 1), ('b', 1), ('a', 1), ('b', 4), ('b', 1), ('a', 1), ('b', 1), ('a', 1), ('b', 4), ('b', 1)], 3)

    result_rdd = rdd.reduceByKey(lambda x, y: x + y)
    # [('b', 3), ('a', 3)]
    print(result_rdd.collect())


def test_map_values(sc: SparkContext) -> None:
    rdd = sc.parallelize([('a', 1), ('b', 2), ('c', 3)], 3)

    result_rdd = rdd.mapValues(lambda v: v * 10)
    # [('a', 10), ('b', 20), ('c', 30)]
    print(result_rdd.collect())


def test_group_by(sc: SparkContext) -> None:
    rdd = sc.parallelize([('a', 1), ('b', 1), ('a', 1), ('a', 1), ('b', 1), ('a', 1)], 3)

    group_rdd = rdd.groupBy(lambda t: t[0])

    # 将value转成list，否则是spark内置的一个iterable对象
    result_rdd = group_rdd.map(lambda t: (t[0], list(t[1])))

    # [('b', [('b', 1), ('b', 1)]), ('a', [('a', 1), ('a', 1), ('a', 1), ('a', 1)])]
    print(result_rdd.collect())


def test_group_by_key(sc: SparkContext) -> None:
    rdd = sc.parallelize([('a', 1), ('b', 1), ('a', 1), ('a', 1), ('b', 1), ('a', 1)], 3)
    group_rdd = rdd.groupByKey()
    # [('b', <pyspark.resultiterable.ResultIterable object at 0x1065d1550>), ('a', <pyspark.resultiterable.ResultIterable object at 0x1065d1670>)]
    print(group_rdd.collect())
    # [('b', [1, 1]), ('a', [1, 1, 1, 1])]
    # 相比于groupBy，groupByKey只将value放到一个组中，不包含key
    print(group_rdd.map(lambda x: (x[0], list(x[1]))).collect())


def test_filter(sc: SparkContext) -> None:
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)

    result_rdd = rdd.filter(lambda x: x % 2 == 0)

    # [2, 4, 6, 8, 10]
    print(result_rdd.collect())


# 去重
def test_distinct(sc: SparkContext) -> None:
    rdd1 = sc.parallelize([1, 1, 1, 1, 2, 2, 3, 4, 2, 2], 3)
    result_rdd1 = rdd1.distinct()
    # [3, 1, 4, 2]
    print(result_rdd1.collect())

    rdd2 = sc.parallelize([('a', 1), ('a', 1), ('b', 1)], 3)
    result_rdd2 = rdd2.distinct()
    # [('a', 1), ('b', 1)]
    print(result_rdd2.collect())

# 将两个rdd合并成一个rdd
def test_union(sc: SparkContext) -> None:
    rdd1 = sc.parallelize([1, 2, 3, 4, 5], 3)
    rdd2 = sc.parallelize([6, 7, 8, 9, 10], 3)

    rdd3 = rdd1.union(rdd2)
    # [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    print(rdd3.collect())

    # rdd1 和 rdd2并没有变
    print(rdd1.collect())
    print(rdd2.collect())


# 只能用于KV型RDD
def test_join(sc: SparkContext) -> None:
    rdd1 = sc.parallelize([(1001, "Jacky"), (1002, "Winfred"), (1003, "Lucy")], 3)
    rdd2 = sc.parallelize([(1001, "Sales"), (1002, "Tech")], 3)

    rdd3 = rdd1.join(rdd2)
    # [(1002, ('Winfred', 'Tech')), (1001, ('Jacky', 'Sales'))]
    print(rdd3.collect())

    rdd4 = rdd1.leftOuterJoin(rdd2)
    # [(1002, ('Winfred', 'Tech')), (1003, ('Lucy', None)), (1001, ('Jacky', 'Sales'))]
    print(rdd4.collect())


# 交集
def test_intersection(sc: SparkContext) -> None:
    rdd1 = sc.parallelize([1, 2, 3, 4, 5], 3)
    rdd2 = sc.parallelize([3, 4, 5, 6, 7], 3)

    rdd3 = rdd1.intersection(rdd2)
    # [3, 4, 5]
    print(rdd3.collect())


# 按分区将item分组，可以一目了然看出一个分区的元素
def test_glom(sc: SparkContext) -> None:
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)
    # [[1, 2, 3], [4, 5, 6], [7, 8, 9, 10]]
    print(rdd.glom().collect())


def test_sort_by(sc: SparkContext) -> None:
    rdd = sc.parallelize([('f', 1), ('e', 3), ('a', 5), ('g', 7), ('b', 2), ('d', 4), ('c', 6)], 3)

    # 按照key排序，升序
    key_sort_rdd = rdd.sortBy(lambda v: v[0], ascending=True)
    print(key_sort_rdd.collect())

    # 按照value排序，降序
    value_sort_add = rdd.sortBy(lambda v: v[1], ascending=False)
    print(value_sort_add.collect())

    # 指定在1个分区内排序，确保全局有序
    # sortBy算子是在Executor上执行的，如果不设置numPartitions=1，只能确保分区内有序，无法确保全局有序
    global_sort_rdd = rdd.sortBy(lambda v: v[0], ascending=True, numPartitions=1)
    print(global_sort_rdd.collect())


def test_sort_by_key(sc: SparkContext) -> None:
    rdd = sc.parallelize([('f', 1), ('e', 3), ('A', 5), ('G', 7), ('b', 2), ('B', 2), ('d', 4), ('c', 6)], 3)
    sort_rdd = rdd.sortByKey(ascending=True, numPartitions=1)

    print(sort_rdd.collect())


if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("transform_operator")
    sc = SparkContext(conf=conf)

    # test_map(sc)

    # test_flat_map(sc)

    # test_reduce_by_key(sc)

    # test_map_values(sc)

    # test_group_by(sc)

    # test_filter(sc)

    # test_distinct(sc)

    # test_union(sc)

    # test_join(sc)

    # test_intersection(sc)

    # test_glom(sc)

    # test_group_by_key(sc)

    # test_sort_by(sc)

    test_sort_by_key(sc)
