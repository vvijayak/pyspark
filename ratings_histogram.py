#!/usr/bin/python
from pyspark_logging import PysparkLogging
from pyspark import SparkContext
from pyspark import SparkConf
import collections, argparse

"""
lines = sc.textFile('data/ml100k/u.data')
ratings = lines.map(lambda x: x.split()[2])
results = ratings.countByValue()

sorted_results = collections.OrderedDict(sorted(results.items()))
for key, value  in sorted_results.iteritems():;
    print '%s %i' % (key,value)
"""



def main():

    #parse args
    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--datafile', help='data file path',required=True)
    args = parser.parse_args()

    logger.info('Data file: %s'% args.datafile)

    #load spark context
    conf = SparkConf().setMaster('local').setAppName('RatingsHistogram')
    sc = SparkContext(conf=conf)
    lines = sc.textFile(args.datafile)

    ratings = lines.map(lambda x: x.split()[2])
    results = ratings.countByValue()

    sorted_results = collections.OrderedDict(sorted(results.items()))
    for key, value  in sorted_results.iteritems():
        print '%s %i' % (key,value)

if __name__ == '__main__':
    logger = PysparkLogging(level='INFO').get_logger()
    main()
