import jieba
from pyspark import SparkConf, SparkContext, StorageLevel

def map_keyword(keyword):
    if keyword == '博学':
        return '博学谷', 1
    if keyword == '传智播':
        return '传智播客', 1
    if keyword == '院校':
        return '院校帮', 1

    return keyword, 1

if __name__ == "__main__":
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    file_rdd = sc.textFile("../data/SogouQ.txt")

    split_rdd = file_rdd.map(lambda line: line.split("\t"))

    # 由于split_rdd会被多次使用，所以这里使用cache
    split_rdd.persist(StorageLevel.DISK_ONLY)

    sentences_rdd = split_rdd.map(lambda line_arr: line_arr[2])

    keywords_rdd = sentences_rdd.flatMap(lambda sentence: jieba.cut_for_search(sentence))

    keywords_with_cnt_rdd = keywords_rdd.filter(lambda keyword: keyword not in ['谷', '帮', '客']).map(map_keyword)

    top5_keywords = keywords_with_cnt_rdd.reduceByKey(lambda x, y: x + y).top(5, lambda item: item[1])

    # ('scala', 2310), ('hadoop', 2268), ('博学谷', 2002), ('传智汇', 1918), ('itheima', 1680)
    print(top5_keywords)


