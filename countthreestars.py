from pyspark import SparkConf,SparkContext
import collections

conf=SparkConf().setMaster("local").setAppName("count_three_stars")
sc=SparkContext(conf=conf)

def parseLines(line):
    a=line.split('\t')
    a_rating=a[2]
    return (a_rating)
    
rdd=sc.textFile("file:///Sparkcourse/ml-100k/u.data")
lines=rdd.map(parseLines)

line=lines.countByValue()

result=collections.OrderedDict(sorted(line.items()))

for a,b in result.items():
    print(a,b)