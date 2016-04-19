#!/usr/bin/python
from pyspark_logging import PysparkLogging
from pyspark import SparkConf, SparkContext
from sys import argv
import os.path

def word_count(text_file_rdd):
    words = text_file_rdd.flatMap(lambda word: word.split())
    return words.count()

def word_histogram(text_file_rdd, wc, f):
    words_hist = text_file_rdd.flatMap(lambda word: word.split()) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b:a + b)\
        .sortBy(lambda (word, count): count)
    for word in words_hist.collect():
        f.write('%s : %s (%.4s)\n' % (word[0], word[1], float(word[1])/float(wc)*100))

def main():
    try:
        file_name = argv[1]
    except:
        raise NameError('Missing command line arg: file_name')
    if os.path.exists(file_name):
        conf = SparkConf().setMaster('local').setAppName('WordCount')
        sc = SparkContext(conf=conf)
        logger.info('Spark Configured')
        text_file_rdd = sc.textFile(file_name)
        logger.info('Loaded '+file_name+' into RDD')

        f = open('word_count.out', 'w')
        wc = word_count(text_file_rdd,)
        word_histogram(text_file_rdd, wc,f)
        f.write('Word Count: %s' % wc)
        f.close()

    else: logger.error('Text file not found...')

if __name__ == '__main__':
    logger = PysparkLogging(level='INFO').get_logger()
    main()
