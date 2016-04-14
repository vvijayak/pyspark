#!/usr/bin/python

from pyspark import SparkContext
from pyspark import SparkConf
import collections

conf = SparkConf().setMaster('local').setAppName('RatingsHistogram')
sc = SparkContext(conf=conf)

lines = sc.textFile('data/ml1m/users.dat')
ratings = lines.map(lambda x: x.split()[0])
result = ratings.countByValue()

outfile = open('output.txt', 'w')
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.iteritems():
    outfile.write("%s %i\n" % (key, value))
