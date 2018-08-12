from pyspark import SparkConf,SparkConfig
import collections

conf=SparkConf().setMaster("local").setAppName("MinTemp")
sc=SparkConfig(conf=conf)

def parseLines(line):
    lines=line.split(',')
    city=lines[0]
    date=lines[1]
    temp=float(lines[3])*0.1
    return (city,date,temp)

rdd=sc.textFile("file:///Sparkcourse/1800.csv")
lines=rdd.map(parseLines)
tmin=lines.filter(lambda x:'TMIN' in x[1])
city_temp=tmin.map(lambda x: x[0],x[2])
city    