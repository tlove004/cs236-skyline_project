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
args = parser.parse_args()


def read_mapper_output(file, separator='\t'):
    for line in file:
        key, value, _  = line.strip('\n').strip(' ').split(separator)
        value = value.split(',')
        value = [float(x) for x in  value]
        yield value

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
If p ranks later than q, then we do not need to check any other values in the
window when sorting is performed first
'''
def ranks_later(p, q):
    rank_p = sum(p)-p[0]
    rank_q = sum(q)-q[0]
    return rank_p > rank_q

'''
As SFS is a subset of BNL, we define the BNL skyline, and rely on pre-sorting for SFS

The first point is added to the window
Every other point p is checked against the window
For SFS, we only check for dominance (those already in the window are definitely in skyline)
For BNL, we have three possibilities:
i.   if p is dominated by any point in the window, it is discarded
ii.  if p dominates any point in the window, it is inserted, and all points in the window dominated by p are deleted, and
iii. if p is neither dominated by, nor dominates, any point in the list, it is inserted
'''
def skyline(values, sfs=False):
    window = [next(values)]
    while True:
        try:
            p = next(values)
        except:
            break
        p_doms = False
        discard = False
        for s in window:
            if dominates(s, p): # case 1
                discard = True
                break
            if sfs:
                if ranks_later(s, p):
                   break
                continue # only need to check for s dom p for SFS
            elif dominates(p, s):
                p_doms = True
        if discard:
            continue;
        if p_doms:
            window[:] = [q for q in window if not dominates(p, q)]
            window.append(p)
        else:
            window.append(p)
    print window



def main(separator='\t'):
    data = read_mapper_output(sys.stdin, separator=separator)
    skyline(data, not args.bnl)


if __name__ == "__main__":
    main()

