from pyspark import SparkConf, SparkContext

# 累加器变量

if __name__ == "__main__":
    conf = SparkConf().setAppName("accumulator").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)

    # Driver中的变量，这个变量会分发给每个分区，但普通变量不会自动在Driver和Executor之间同步，在Executor中对该变量的修改不会同步回Driver
    count = 0

    # 累加器变量可以在Executor和Driver之间自动同步变量的变化
    ac_cnt = sc.accumulator(0)

    def map_func(data):
        # global count
        # count += 1
        # print(count)

        global ac_cnt
        ac_cnt += 1
        print(ac_cnt)

    rdd.map(map_func).collect()

    # 0，这里打印的Driver中的count变量，Driver中的count变量没有任何变化
    # print(count)

    # 10
    print(ac_cnt)