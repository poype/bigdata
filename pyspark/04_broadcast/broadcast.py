import time

from pyspark import SparkConf, SparkContext

# 广播变量

if __name__ == "__main__":
    conf = SparkConf().setAppName("Broadcast").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 这是一个本地对象
    stu_info_list = [
        (1, "张三", 11),
        (2, "李四", 13),
        (3, "王五", 11),
        (4, "赵六", 11)
    ]

    # 将普通对象分装到广播变量中
    broadcast_stu_info_list = sc.broadcast(stu_info_list)

    # 这是一个RDD对象
    score_info_rdd = sc.parallelize([
        (1, "语文", 99),
        (2, "数学", 99),
        (3, "英语", 99),
        (4, "编程", 99),
    ])


    def map_func(data):
        id = data[0]
        name = ''

        # 从广播变量中结构中真正的对象
        stu_info_list_copy = broadcast_stu_info_list.value

        # for stu_item in stu_info_list:  用广播变量中的对象替换本地对象
        for stu_item in stu_info_list_copy:
            if id == stu_item[0]:
                name = stu_item[1]
                return name, data[1], data[2]

    print(score_info_rdd.map(map_func).collect())

# 除了RDD部分，其它都是由Driver去运行的，所以这里的本地对象stu_info_list是在Driver中的对象。
# RDD的相关代码（包括这里的map_func方法）是在Executor中分布式执行的
# 在RDD map_func 的执行过程中，需要依赖本地对象stu_info_list，所以Driver会以网络传输的方式将stu_info_list对象传送到每个Executor中的所有分区。
# 如果一个Executor上有4个分区，那么默认就会有4份stu_info_list对象被传输到那个Executor上。

# 这样就存在资源浪费，一个Executor上可以包含多个分区，但为每个分区都单独传送一份stu_info_list对象
# 一个Executor上只要传送一份stu_info_list对象就可以了，这就需要broadcast对象

# 广播变量可以避免这个问题，它可以确保针对每个Executor，相同的变量只会传输一次，同一个Executor中的分区task可以共享广播变量中的value，而无需Driver给每个分区task单独发送一次。