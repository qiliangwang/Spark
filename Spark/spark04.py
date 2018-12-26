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
    my_map()
    sc.stop()


if __name__ == '__main__':
    main()
