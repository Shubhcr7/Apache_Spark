from pyspark import SparkConf,SparkContext
import collections

conf=SparkConf().setMaster("local").setAppName("word count")
sc=SparkContext(conf=conf)

rdd=sc.textFile("file:///Sparkcourse/book.txt")
words=rdd.flatMap(lambda x: x.split())

total_words=words.countByValue()

for word,count in total_words.items():
    clean=word.encode('ascii','ignore')
    if(clean):
        print (clean,word)