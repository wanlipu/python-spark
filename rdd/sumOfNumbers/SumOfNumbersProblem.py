import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    '''
    Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
    print the sum of those numbers to console.
    Each row of the input file contains 10 prime numbers separated by spaces.
    '''
    conf = SparkConf().setAppName("reduce").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("in/prime_nums.text")
    numbers = lines.flatMap(lambda line: line.split("\t"))

    validNumbers = numbers.filter(lambda number: number)
    intNumbers = validNumbers.map(lambda number: int(number))


    product = intNumbers.reduce(lambda x, y: x + y)
    print("product is :{}".format(product))