__author__  = "itohiro73"

import time
from pyspark import SparkContext

sc = SparkContext("local", "Temperature Analysis App")
logData = sc.textFile("/home/spark/input/stats").cache()

def mapToTriplet(s):
    columns = s.split(",")
    return (columns[0], time.strptime(columns[2], "%Y-%m-%d %H:%M:%S.%f"), columns[3])

hostToRecords = logData.map(mapToTriplet)

hostToRecords.saveAsTextFile("/home/spark/output/stats")
