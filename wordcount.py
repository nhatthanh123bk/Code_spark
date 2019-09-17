from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount_partition=8")\
        .getOrCreate()

    lines = spark.read.text("/nhatthanh/data/wordcount.txt").rdd.map(lambda r: r[0])
    lines= lines.repartition(8)
    print(lines.getNumPartitions)
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    df_ratings = spark.read.format("csv").load("/nhatthanh/data/movie_len1M/movies.csv")
    df_ratings.show()
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark.stop()
