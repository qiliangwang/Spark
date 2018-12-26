from pyspark import SparkConf, SparkContext


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("spark0401")
    sc = SparkContext(conf=conf)

    def my_map():
        data = [1, 2, 3, 4, 5]
        rdd = sc.parallelize(data)
        result = rdd.map(lambda x: x * 2).collect()
        print(result)
        pass

    def my_filter():
        data = [1, 2, 3, 4, 5]
        rdd = sc.parallelize(data)
        result = rdd.map(lambda x: x * 2).filter(lambda x: x > 5).collect()
        print(result)
        pass

    def my_flat_map():
        data = ['hello spark', 'hello java', 'hello python', 'hello kafka', 'hello storm']
        rdd = sc.parallelize(data)
        # [['hello', 'spark'], ['hello', 'java'], ['hello', 'python'], ['hello', 'kafka'], ['hello', 'storm']]
        print(rdd.map(lambda line: line.split(' ')).collect())
        # ['hello', 'spark', 'hello', 'java', 'hello', 'python', 'hello', 'kafka', 'hello', 'storm']
        print(rdd.flatMap(lambda line: line.split(' ')).collect())
        split__map = rdd.flatMap(lambda line: line.split(' ')).map(lambda x: (x, 1))
        print(split__map.collect())
        pass

    def my_group_by_key():
        data = ['hello spark', 'hello java', 'hello python', 'hello kafka', 'hello storm']
        rdd = sc.parallelize(data)
        map_rdd = rdd.flatMap(lambda line: line.split(' ')).map(lambda x: (x, 1))
        # [{'java': [1]}, {'python': [1]}, {'storm': [1]}, {'hello': [1, 1, 1, 1, 1]}, {'spark': [1]}, {'kafka': [1]}]
        print(map_rdd.groupByKey().map(lambda x: {x[0]: list(x[1])}).collect())

    def my_reduce_by_key():
        data = ['hello spark', 'hello java', 'hello python', 'hello kafka', 'hello storm']
        rdd = sc.parallelize(data)
        map_rdd = rdd.flatMap(lambda line: line.split(' ')).map(lambda x: (x, 1))
        # [{'java': [1]}, {'python': [1]}, {'storm': [1]}, {'hello': [1, 1, 1, 1, 1]}, {'spark': [1]}, {'kafka': [1]}]
        group_rdd = map_rdd.reduceByKey(lambda a, b: a + b)
        result = group_rdd.map(lambda x: (x[1], x[0])).sortByKey(False).map(lambda x: (x[1], x[0]))
        print(result.collect())

    def group_by_key_word_count():
        data = ['hello spark', 'hello java', 'hello python', 'hello kafka', 'hello storm']
        rdd = sc.parallelize(data)
        map_rdd = rdd.flatMap(lambda line: line.split(' ')).map(lambda x: (x, 1))
        result = map_rdd.groupByKey().map(lambda x: {x[0]: sum(list(x[1]))})
        print(result.collect())

    def my_union():
        pass

    my_reduce_by_key()
    sc.stop()


def word_count(file_dir):
    conf = SparkConf().setMaster("local[2]").setAppName("spark0401")
    sc = SparkContext(conf=conf)
    counts = sc.textFile(file_dir)\
        .flatMap(lambda line: line.split('   '))\
        .map(lambda x: (x, 1))\
        .reduceByKey(lambda a, b: a + b)
    output = counts.collect()
    print(output)
    sc.stop()
    pass


def save_file(file_dir, save_dir):
    conf = SparkConf().setMaster("local[2]").setAppName("spark0401")
    sc = SparkContext(conf=conf)
    counts = sc.textFile(file_dir) \
        .flatMap(lambda line: line.split('   ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b).saveAsTextFile(save_dir)
    # output = counts.collect()
    # print(counts)
    sc.stop()


def topN(file_dir):
    conf = SparkConf().setMaster("local[2]").setAppName("spark0401")
    sc = SparkContext(conf=conf)
    counts = sc.textFile(file_dir) \
        .flatMap(lambda line: line.split('   ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: (x[1], x[0])).sortByKey(False).map(lambda x: (x[1], x[0])).take(5)
    # output = counts.collect()
    print(counts)
    sc.stop()


def average(file_dir):
    conf = SparkConf().setMaster("local[2]").setAppName("spark0401")
    sc = SparkContext(conf=conf)
    data = sc.textFile(file_dir)
    data = data.map(lambda line: line.split(' ')[1])
    total = data.map(lambda age: int(age)).reduce(lambda a, b: a + b)
    avg = total / data.count()
    print(avg)


if __name__ == '__main__':
    # main()
    # word_count('file:///home/vaderwang/PycharmProjects/Spark/*.txt')
    # save_file('file:///home/vaderwang/PycharmProjects/Spark/*.txt', 'file:///home/vaderwang/PycharmProjects/Spark/wc')
    # topN('file:///home/vaderwang/PycharmProjects/Spark/hello1.txt')
    average('file:///home/vaderwang/PycharmProjects/Spark/hello1.txt')
