#importing the necessary libraries
from pyspark import SparkConf,SparkContext
import collections

#confguring the spark job
conf=SparkConf().setMaster("local").setAppName("avgfriends")
sc=SparkContext(conf=conf)

#this function returns a key,value tuple 
def parseLine(line):
    data=line.split(',')
    age=int(data[2])
    friends=int(data[3])
    return (age,friends)

#reading the file via spark context    
lines=sc.textFile('file:///Sparkcourse/fakefriends.csv')

#creating the RDD
rdd=lines.map(parseLine)

#mappingvalues (33,355)--> (33,(355,1)) && (33,244) ---> (33,(245,1)) then reducing by key --> (33,(600,2))
totalsByAge=rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))

#computing average (33,(600,2))--> 600/2
averagesByAge=totalsByAge.mapValues(lambda x: int(x[0]/x[1]))

#gathering all such result by converting the RDD into list of tuples
results=averagesByAge.collect()

#printing the desired ouput
for result in results:
    print(result)