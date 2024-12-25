import time

from pyspark import SparkConf, SparkContext
import json

def split_line(line):
    print("split_line executed")
    return line.split('|')

def convert_json_to_dict(json_str):
    print("convert_json_to_dict executed")
    return json.loads(json_str)

def map_dict_to_city_category(dict_obj: dict):
    print("map_dict_to_city_category executed")
    return dict_obj['areaName'], dict_obj['category']

def map_dict_to_city_money(dict_obj: dict):
    print("map_dict_to_city_money executed")
    return dict_obj['areaName'], dict_obj['money']

if __name__ == "__main__":
    conf = SparkConf().setAppName("test_cache")
    sc = SparkContext(conf=conf)

    line_rdd = sc.textFile("hdfs://node1:8020/input/order.txt")

    json_rdd = line_rdd.flatMap(split_line)

    # 将json转换成dict对象
    dict_rdd = json_rdd.map(convert_json_to_dict)
    dict_rdd.cache()   # 缓存 dict_rdd, 这样 dict_rdd就不用被计算两次了

    city_category_rdd = dict_rdd.map(map_dict_to_city_category)

    # 筛选出北京的item
    beijing_category_rdd = city_category_rdd.filter(lambda item: item[0] == '北京')

    result_rdd = beijing_category_rdd.distinct()

    # 获取北京所售卖的所有category
    print(result_rdd.collect())

    # 每个城市的销售总金额
    city_sum_money_rdd = dict_rdd.map(map_dict_to_city_money).reduceByKey(lambda x, y: int(x) + int(y))
    print(city_sum_money_rdd.collect())

    # hold住job，为了看monitor
    time.sleep(160)

    # 不要忘记删除缓存
    dict_rdd.unpersist()



# line_rdd -> json_rdd -> dict_rdd -> city_category_rdd -> beijing_category_rdd
#                               |
#                               | --> city_sum_money_rdd

# 这里 dict_rdd 被两条链路所依赖，如果不做任何缓存处理，line_rdd、json_rdd、dict_rdd 这三个RDD将会被计算两次
# 所以这个程序提交后将会创建两个Job(对应两个action算子)，每个job都对应一个完整的RDD链条

# rdd.cache()    缓存到内存
# rdd.persist(StorageLevel.MEMORY_ONLY)         仅内存缓存
# rdd.persist(StorageLevel.MEMORY_ONLY_2)       仅内存缓存, 2个副本
# rdd.persist(StorageLevel.DISK_ONLY)           仅缓存在磁盘
# rdd.persist(StorageLevel.MEMORY_AND_DISK)     先放内存，不够放硬盘
# 除了上面这些缓存策略外，还有其它一些缓存策略

# rdd.unpersist()   清理缓存

# 无论是保存在内存还是硬盘，缓存是有可能丢失的