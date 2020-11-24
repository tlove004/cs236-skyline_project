#!/usr/bin/python
"""We implement a two-step mapreduce job:
step1: partition data into N sets and compute local skylines
step2: merges local skylines to compute global skyline

We implement BNL with optional SFS, and provide an optional angle-based partitioner
"""
#from skyutil import skyline
import sys
import argparse


parser = argparse.ArgumentParser(description='get number of reducers to map to')
parser.add_argument('-b','--BNL', help="Perform basic branch-nested-loop (without sorting)", default=False, required=False, action='store_true', dest='bnl')
parser.add_argument('-d','--DIM', help='number of dimensions', default=2,
        required=False, type=int, dest='dim')
args = parser.parse_args()

y_min = float("inf")

def read_mapper_output(file, separator='\t'):
    for line in file:
        key, value, _  = line.strip('\n').strip(' ' ).split(separator)
        value = value.split(',')
        value = [float(x) for x in  value]
        yield value

'''
implementation of B-tree for 3d skyline, adapted from:
   https://www.tutorialspoint.com/python/python_binary_search_tree.htm
we store index, value pairs
'''
class Node:
    def __init__(self, index, data):

        self.left = None
        self.right = None
        self.index = index
        self.data = data

# Insert method to create nodes
    def insert(self, index, data):
        if self.data:
            if index < self.index:
                if self.left is None:
                    self.left = Node(index, data)
                else:
                    self.left.insert(index, data)
            elif index > self.index:
                if self.right is None:
                    self.right = Node(index, data)
                else:
                    self.right.insert(index, data)
        else:
            self.data = data
            self.index = index

# findval method to compare the value with nodes
# lkpval is an index
    def findval(self, lkpval):
        if lkpval < self.index:
            if self.left is None:
                return False
            return self.left.findval(lkpval)
        elif lkpval > self.index:
            if self.right is None:
                return False
            return self.right.findval(lkpval)
        else:
            return self.data

    def findpred(self, lkpval):
        if self.index > lkpval:
            # go left if possible
            if self.left is None:
                return False
            return self.left.findpred(lkpval)
        elif self.index < lkpval:
            if self.right is None:
                # found highest indexed node less than lkpval
                return self.data
            return self.right.findpred(lkpval)
        else: # self.index == lkpval
            return self.data

    def findsucc(sel, lkpval):
        if self.index < lkpval:
            # go right if possible
            if self.right is None:
                return False
            return self.right.findsucc(lkpval)
        elif self.index > lkpval:
            if self.left is None:
                # found lowest indexed node greater than lkpval
                return self.data
            return self.left.findsucc(lkpval)
        else: # self.index == lkpval
            return self.data



'''
We say that p dominates q iff for each attribute i in [1, d], p_i <= q_i and at least 1 p_j < q_j
In other words, over all dimensions, p is not worse than q, and better than q in at least one dim
'''
def dominates(p, q):
    found = False
    for i in range(1, len(p)):
        if p[i] > q[i]:
            return False
        if p[i] < q[i]:
            found = True
    return found


'''
the 2-d skyline algorithm, which runs in O(nlogn) time does not need to
strictly check for dominance
'''
def skyline2d(values):
    p = next(values)
    y_min = p[2]
    SKY = [p]
    for p in values:
        if p[2] < y_min:
            SKY.append(p)
            y_min = p[2]
    print SKY


'''
the 3-d skyline algorithm,
 -- no point that ranks after in the sorted list can dominate an earlier point
 -- SKY_yz = skyline of projections of S on the yz plane.
    --- a point in P dominates a point p in the x-y-z space iff a point of
    SKY_yz dominates p in the yz-plane.

 we index points of SKY_yz by their y-coordinates using a binary tree
'''
def skyline3d(values):
    for p in values:
        print p




def main(separator='\t'):
    data = read_mapper_output(sys.stdin, separator=separator)
    if args.dim == 2:
        skyline2d(data)
    else:
        skyline3d(data)


if __name__ == "__main__":
    main()

