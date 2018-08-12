from pyspark import SparkConf,SparkContext
# import collections
import re
conf=SparkConf().setMaster("local").setAppName("word-count-better-sorted")
sc=SparkContext(conf=conf)

def normalizeWords(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower())
    
text=sc.textFile("file:///Sparkcourse/book.txt")
words=text.flatMap(normalizeWords)

wordCounts=words.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y)
wordCountsSorted=wordCounts.map(lambda x:(x[1],x[0])).sortByKey()

results=wordCountsSorted.collect()

print (results)
for result in results:
    count=str(result[0])
    word=result[1].encode('ascii','ignore')
    if(word):
        print("{} \t {}".format(word,count))

        