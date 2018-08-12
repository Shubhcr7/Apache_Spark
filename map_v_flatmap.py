from pyspark import SparkConf,SparkContext
import collections

conf=SparkConf().setMaster("local").setAppName("map_v_flatMap")
sc=SparkContext(conf=conf)

rdd=sc.textFile("file:///Sparkcourse/mappp.txt")
rdd1=rdd.flatMap(lambda x:x.split())
text=rdd1.collect()

print(text) 