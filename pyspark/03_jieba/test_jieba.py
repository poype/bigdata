import jieba


if __name__ == '__main__':
    content = '小明硕士毕业于中国科学院计算所，然后在清华大学深造'
    # 分词
    result = jieba.cut(content, True)
    print(list(result))
    print(type(result))

# jieba是Python中的一个分词库