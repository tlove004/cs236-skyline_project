#!/bin/bash

#runner.sh inputs:
# input file, intermediate results location, final results location, dimensions

declare -a SIZES=("32" "64")
declare -a NUM=("4" "3" "2" "1")
declare -a ATTR=("2" "3")
declare -a TYPE=("i" "c" "a")
declare -a PART=("" "-p")
declare -a BNL=("" "-b")

for size in "${SIZES[@]}"
do
	for num in "${NUM[@]}"
	do
    	for attr in "${ATTR[@]}"
    	do
        	for _type in "${TYPE[@]}"
        	do 
				NAME=$(($size * 2**20))
				if [ ! -f ${_type}_${NAME}_${attr}.csv ]; then
				    echo "Generating ${_type}_${NAME}_${attr}.csv..."
				    source gendata.sh ${size} ${attr} ${_type}
				else
				    echo "Verifying file ${_type}_${NAME}_${attr}.csv..."
				fi
				echo "Done... copying file to HDFS..."
				hadoop fs -put ${_type}_${NAME}_${attr}.csv
				for part in "${PART[@]}"
				do
					if [ ! -f results/${_type}_${NAME}_${attr}_${num}${part}-x/_SUCCESS ]
					then	
						echo 'Preparing to run: ./runner.sh '"${_type}"'_'"${NAME}"'_'"${attr}"'.csv mid/'"${_type}"'_'"${NAME}"'_'"${attr}"' res/'"${_type}"'_'"${NAME}"'_'"${attr}"' '"$(($attr + 2))"' '"${num}"' '"${part:-\"\"}"' \"\" -h '\''-d '"${attr}"\'' -x  2>&1 | tee results/'"${_type}"'_'"${NAME}"'_'"${attr}"'_'"${num}${part}"'-x/run.log'

						echo "Staging results/${_type}_${NAME}_${attr}_${num}${part}-x/ for results..."
						mkdir results/${_type}_${NAME}_${attr}_${num}${part}-x
						echo "Done... starting map-reduce job..."
						./runner.sh ${_type}_${NAME}_${attr}.csv mid/${_type}_${NAME}_${attr} res/${_type}_${NAME}_${attr} $(($attr + 2)) ${num} ${part:-\"\"} "" -h '-d '"${attr}"' ' -x  2>&1 | tee results/${_type}_${NAME}_${attr}_${num}${part}-x/run.log
						#TODO: capture return value, check for success/failure, add success/failure to a log file
						echo "Done... copying results from HDFS to local"
						hadoop fs -get /tmp/res/${_type}_${NAME}_${attr}/* results/${_type}_${NAME}_${attr}_${num}${part}-x/
				        fi
					for bnl in "${BNL[@]}"
					do
						if [ -f results/${_type}_${NAME}_${attr}_${num}${part}${bnl}/_SUCCESS ]
						then continue
						fi	
						echo 'Preparing to run: ./runner.sh '"${_type}"'_'"${NAME}"'_'"${attr}"'.csv mid/'"${_type}"'_'"${NAME}"'_'"${attr}"' res/'"${_type}"'_'"${NAME}"'_'"${attr}"' '"$(($attr + 2))"' '"${num}"' '"${part:-\"\"}"' '"${bnl:-\"\"}"' 2>&1 | tee results/'"${_type}"'_'"${NAME}"'_'"${attr}"'_'"${num}${part}${bnl}"'/run.log'
						sleep 10

						echo "Staging results/${_type}_${NAME}_${attr}_${num}${part}${bnl}/ for results..."
						mkdir results/${_type}_${NAME}_${attr}_${num}${part}${bnl}
						echo "Done... starting map-reduce job..."
						./runner.sh ${_type}_${NAME}_${attr}.csv mid/${_type}_${NAME}_${attr} res/${_type}_${NAME}_${attr} $(($attr + 2)) ${num} ${part:-\"\"} ${bnl:-\"\"} 2>&1 | tee results/${_type}_${NAME}_${attr}_${num}${part}${bnl}/run.log
						#TODO: capture return value, check for success/failure, add success/failure to a log file
						echo "Done... copying results from HDFS to local"
						hadoop fs -get /tmp/res/${_type}_${NAME}_${attr}/* results/${_type}_${NAME}_${attr}_${num}${part}${bnl}/
					done
				done
				echo "Cleaning up..."
				rm ${_type}_${NAME}_${attr}.csv
				hadoop fs -rm ${_type}_${NAME}_${attr}.csv
	        	echo "Done."
			done
        done
    done
done
