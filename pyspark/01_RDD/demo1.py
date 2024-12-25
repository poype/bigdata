# 读取order.txt文件，获取北京售卖的商品类别信息
from pyspark import SparkConf, SparkContext
import json

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]").setAppName("demo1")
    sc = SparkContext(conf=conf)

    line_rdd = sc.textFile("../data/order.txt")

    json_rdd = line_rdd.flatMap(lambda line: line.split("|"))

    # 将json转换成dict对象
    dict_rdd = json_rdd.map(lambda x: json.loads(x))

    city_category_rdd = dict_rdd.map(lambda dict: (dict['areaName'], dict['category']))

    # 筛选出北京的item
    beijing_category_rdd = city_category_rdd.filter(lambda item: item[0] == '北京')

    result_rdd = beijing_category_rdd.distinct()

    print(result_rdd.collect())