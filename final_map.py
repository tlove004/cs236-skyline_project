#!/usr/bin/python
"""We implement a two-step mapreduce job:
   step1: partition data into N sets and compute local skylines
   step2: merges local skylines to compute global skyline

   We implement BNL with optional SFS, and provide an optional angle-based partitioner
   """

from random import randint
import sys, ast
import argparse
parser = argparse.ArgumentParser(description='Parse the args!')
parser.add_argument('-x','--heuristic',help="Use nlogn heuristic",
        default=False, action='store_true', dest='heuristic')

args=parser.parse_args()

fieldsep='$'

def read_input(file):
    for values in file:
        values = ast.literal_eval(values)
        for value in values:
            yield value

def main(separator='\t'):
    data = read_input(sys.stdin)
    for value in data:
        if args.heuristic:
            rank = value[1]
        else:
            rank = sum(value)-value[0]
        if len(value) == 3:
            print "%d%s%.16f%s%.16f%s%.16f%s%d,%.16f,%.16f" % (1, fieldsep,
                    rank, fieldsep, value[1],
                    fieldsep, value[2], separator, value[0], value[1], value[2])
        else:
            print "%d%s%.16f%s%.16f%s%.16f%s%.16f%s%d,%.16f,%.16f,%.16f" % (1,
                    fieldsep, rank, fieldsep, value[1], fieldsep, value[2], fieldsep, value[3],
                    separator, value[0], value[1], value[2], value[3])

if __name__ == "__main__":
    main()
