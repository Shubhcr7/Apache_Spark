from pyspark import SparkConf,SparkContext
import collections

conf=SparkConf().setMaster("local").setAppName("Popular Movie")
sc=SparkContext(conf=conf)

movies=sc.textFile("file:///Sparkcourse/ml-100k/u.data")
movie_id=movies.map(lambda x: (int(x.split()[1]),1))
movie_id_count=movie_id.reduceByKey(lambda x,y:x+y)
flipped=movie_id_count.map(lambda x:(x[1],x[0]))
sortedMovie=flipped.sortByKey()
results=sortedMovie.collect()

for result in results:
    print (result)
