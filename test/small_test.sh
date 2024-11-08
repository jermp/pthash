# /usr/bin/bash

# num_keys: $1
# avg_partition_size: $2
# num_threads: $3

./build -n $1 -l 3.5 -a 0.94 -r add -b skew -e D-D -s 727369 --verbose --check --minimal -q 1000 -p 100 -t 1
./build -n $1 -l 3.5 -a 0.94 -r add -b opt -e R-R -s 727369 --verbose --check --minimal -q 1000 -p 1000 -t $3
./build -n $1 -l 3.5 -a 0.94 -r add -b skew -e D-D -s 727369 --verbose --check --minimal -q 1000 -p $2 -t 1
./build -n $1 -l 3.5 -a 0.94 -r add -b opt -e R-R -s 727369 --verbose --check --minimal -q 1000 -p $2 -t $3

./build -n $1 -l 3.5 -a 0.94 -r add -b skew -e D-D -s 727369 --verbose --check --minimal -q 1000 -p 100 -t 1 --external
./build -n $1 -l 3.5 -a 0.94 -r add -b opt -e R-R -s 727369 --verbose --check --minimal -q 1000 -p 1000 -t $3 --external
./build -n $1 -l 3.5 -a 0.94 -r add -b skew -e D-D -s 727369 --verbose --check --minimal -q 1000 -p $2 -t 1 --external
./build -n $1 -l 3.5 -a 0.94 -r add -b opt -e R-R -s 727369 --verbose --check --minimal -q 1000 -p $2 -t $3 --external