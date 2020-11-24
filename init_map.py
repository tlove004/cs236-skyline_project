#!/usr/bin/python
"""We implement a two-step mapreduce job:
   step1: partition data into N sets and compute local skylines
   step2: merges local skylines to compute global skyline

   We implement BNL with optional SFS, and provide an optional angle-based partitioner
   """

from random import randint
import sys
import argparse
from math import atan2, sqrt, pi, ceil

parser = argparse.ArgumentParser(description='get number of reducers to map to')
parser.add_argument('-n','--num', help="Number of reducers we are mapping to. Optional. Default=4.", default=4, type=int, required=False,
        dest='num')
parser.add_argument('-p','--partitioner', help="Turn on angle-based partitioning", default=False, required=False,
action='store_true', dest='angle')
parser.add_argument('-x','--heuristic', help='nlogn implementation for 2/3d',
        default=False, required=False, action='store_true', dest='heuristic')

args=parser.parse_args()

mapnum=args.num
fieldsep='$'

def read_input(file):
    for line in file:
        yield line.split(',')

def main(separator='\t'):
    data = read_input(sys.stdin)
    if args.angle:
       sweep = float(float(90)/args.num) # how much we'll sweep a partition [2d]
       parts_xy = int(ceil((sqrt(args.num))))
       parts_xz = parts_xy
       if parts_xy*parts_xz > args.num:
           if parts_xy*(parts_xz-1) >= args.num:
              parts_xz = parts_xz-1
           elif (parts_xy+1)*(parts_xz-1) >= args.num:
               parts_xy = parts_xy+1
               parts_xz = parts_xz-1
       sweep_xy = float(float(90)/parts_xy)
       sweep_xz = float(float(90)/parts_xz)
       rad_to_deg = float(180)/pi
    for point in data:
        value = point[:-1] # remove string padding
        value = [float(x) for x in value]
        if args.heuristic:
            rank = value[1]
        else:
            rank = sum(value)-value[0]
        if len(value) == 3 and args.angle: # angle partitioning for 2d
            if value[1] is 0: #quick hack
                part = args.num
            else:
                angle = atan2(float(value[2]),float(value[1]))*rad_to_deg
                for i in range(0, args.num):
                    if angle <= (i+1)*sweep:
                        part = i
                        break
            print"%d%s%.16f%s%.16f%s%.16f%s%d,%.16f,%.16f" % (part,
                    fieldsep, rank, fieldsep, value[1], fieldsep, value[2],
                    separator, value[0], value[1], value[2])
        elif len(value) == 3:
            print "%d%s%.16f%s%.16f%s%.16f%s%d,%.16f,%.16f" % (randint(1, mapnum),
                    fieldsep, rank, fieldsep, value[1], fieldsep, value[2], separator, value[0], value[1], value[2])
        elif args.angle: # angle partitioning for 3d:
            ''' partition works as follows:
                  - we first estimate the parts per dimension through
                  sqrt(args.num) and varying xy/xz parts to get close as
                  possible through even dividing of each plane [realistically,
                  our approach is only guaranteed to evenly partition the
                  volume if the number of partitions desired is a power of 2]
                  - we next find the sweep over the xy/xz plane that partitions
                  the discovered parts per dimension
                  - we find the correct partition as follows:
                     - let phi_xy = the point's angle on the xy plane
                     - let phi_xz = the point's angle on the xz plane
                     - we find part_xy(xz) by looping through each sweep partition
                     for each plane, and combine part_xy and part_xz to find a
                     unique partition.
                     - in order to avoid indadvertently assigning to the same
                      partition (ex part_xy = 1, part_xz = 2 vs. part_xy = 2,
                     part _xz = 1), we weigh part_xz by sqrt(args.num).  If we desire
                     9 partitions, we will get 5 for the first scenario, and 7
                     for the second.  the ninth parittion would have xy=2 xz=2
                     -> partition_num = 2*3+2 = 8
                     it is important to note that the cluster we are testing on
                     only supports a maximum of 4 partitions.  we have tested
                     for partitioning [1, 2, 3, 4] and have verified the
                     algorithm accordingly.
            '''
            phi_xy = atan2(float(value[2]),float(value[1]))*rad_to_deg
            phi_xz = atan2(float(value[3]),float(value[1]))*rad_to_deg
            for i in range(0, parts_xy):
                if phi_xy <= (i+1)*sweep_xy:
                    part_xy = i
                    break
            for i in range(0, parts_xz):
                if phi_xz <= (i+1)*sweep_xz:
                    part_xz = i
                    break
            part = part_xy*(int(sqrt(args.num)))+part_xz
            #print "%.4f, %.4f, %d, %d, %d, %d, %d, %.4f, %.4f" % (phi_xy,
            #       phi_xz, part_xy, part_xz, part, parts_xy, parts_xz, sweep_xy, sweep_xz)
            print "%d%s%.16f%s%.16f%s%.16f%s%.16f%s%d,%.16f,%.16f,%.16f" % (part,
                    fieldsep, rank, fieldsep, value[1],
                fieldsep, value[2], fieldsep, value[3], separator,
                value[0], value[1], value[2], value[3])
        else:
            print "%d%s%.16f%s%.16f%s%.16f%s%.16f%s%d,%.16f,%.16f,%.16f" % (randint(1,
                mapnum), fieldsep, rank, fieldsep, value[1], fieldsep, value[2], fieldsep, value[3], separator,
                value[0], value[1], value[2], value[3])


if __name__ == "__main__":
    main()
