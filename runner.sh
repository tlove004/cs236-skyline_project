#!/bin/bash

#file_in = $1
#midout = $2
#out = $3
#dim+2 = $4
#reducers = $5
#angle partitioning = $6 [-p]
#use basic (no sorting) BNL = $7 [-b]
#use experimental reducer = $8 [-h]
#when using experimental reducer, must indicate dimensions = $9 [-d {2, 3}]
#when using experimental reducer, must indicate so for mappers = $10 [-x]


#    -reducer org.apache.hadoop.mapred.lib.IdentityReducer \
if [[ "$8" == "-h" && $4 -eq 5 ]]; then
	echo "NOT YET IMPLEMENTED"
	hadoop fs -rm -r /tmp
	exit 1
fi

echo -mapper 'init_map.py -n '"${5}"' '"${6}"' '"${10}"
echo -reducer 'reducer'"${8}"'.py '"${7}"' '"${9}"

hadoop fs -rm -r /tmp

hadoop jar /usr/local/src/hadoop-2.7.7/share/hadoop/tools/lib/hadoop-streaming-2.7.7.jar \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
    -D stream.map.output.field.separator=$ \
    -D stream.num.map.output.key.fields=$4 \
    -D mapreduce.map.output.key.field.separator=$ \
    -D mapreduce.partition.keypartitioner.options=-k1,1 \
    -D mapreduce.partition.keycomparator.options=-k2n \
    -D mapreduce.job.reduces=$5 \
    -input $1 \
    -file init_map.py \
    -file reducer.py \
    -file reducer-h.py \
    -output /tmp/$2 \
    -mapper 'init_map.py -n '"$((${5}**2))"' '"${6}"' '"${10}" \
    -reducer 'reducer'"${8}"'.py '"${7}"' '"${9}" \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner

    test $5 -eq 1 && hadoop fs -mkdir /tmp/res && hadoop fs -mkdir /tmp/$3 && hadoop fs -cp /tmp/$2/* /tmp/$3/ && hadoop fs -cat /tmp/$3/* \
    || hadoop fs -ls /tmp/$2/_SUCCESS \
    && hadoop jar /usr/local/src/hadoop-2.7.7/share/hadoop/tools/lib/hadoop-streaming-2.7.7.jar \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
    -D stream.map.output.field.separator=$ \
    -D stream.num.map.output.key.fields=$4 \
    -D mapreduce.map.output.key.field.separator=$ \
    -D mapreduce.partition.keypartitioner.options=-k1,1 \
    -D mapreduce.partition.keycomparator.options=-k2n \
    -D mapreduce.job.reduces=1 \
    -input /tmp/$2 \
    -file final_map.py \
    -file reducer.py \
    -file reducer-h.py \
    -output /tmp/$3 \
    -mapper 'final_map.py '"${10}" \
    -reducer 'reducer'"${8}"'.py '"${7}"' '"${9}" \
    && hadoop fs -cat /tmp/$3/* \
    || echo "Skipping second step, first step failed, or only using a single reducer."
