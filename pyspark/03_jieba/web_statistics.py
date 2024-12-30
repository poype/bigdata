from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("web_statistics").setMaster("local[*]")
    sc = SparkContext(conf=conf)


    file_rdd = sc.textFile("../data/apache.log")

    file_rdd.cache()

    # 1. PV of whole site
    print(file_rdd.count())

    arr_rdd = file_rdd.map(lambda line: line.split(" "))
    arr_rdd.cache()

    user_rdd = arr_rdd.map(lambda arr: arr[1]).distinct(1)

    # 2. 访问网站的用户数量
    print(user_rdd.count())

    ip_rdd = arr_rdd.map(lambda arr: arr[0]).distinct(1)

    # 3. ip
    print(ip_rdd.distinct().collect())

    # 4. 访问次数最多的页面
    page_cnt = arr_rdd.map(lambda arr: (arr[4], 1)).reduceByKey(lambda x, y: x + y).top(1, lambda page_cnt: page_cnt[1])
    print(page_cnt)


