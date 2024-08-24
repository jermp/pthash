# /usr/bin/bash

# num_keys: $1
# num_partitions: $2
# num_threads: $3

./build -n $1 -c 3.5 -a 0.94 -e dictionary_dictionary -s 727369 --minimal --verbose --check --lookup -p 1 -t 1
./build -n $1 -c 3.5 -a 0.94 -e dictionary_dictionary -s 727369 --minimal --verbose --check --lookup -p 1 -t $3
./build -n $1 -c 3.5 -a 0.94 -e dictionary_dictionary -s 727369 --minimal --verbose --check --lookup -p $2 -t 1
./build -n $1 -c 3.5 -a 0.94 -e dictionary_dictionary -s 727369 --minimal --verbose --check --lookup -p $2 -t $3

./build -n $1 -c 3.5 -a 0.94 -e dictionary_dictionary -s 727369 --minimal --verbose --check --lookup -p 1 -t 1 --external
./build -n $1 -c 3.5 -a 0.94 -e dictionary_dictionary -s 727369 --minimal --verbose --check --lookup -p 1 -t $3 --external
./build -n $1 -c 3.5 -a 0.94 -e dictionary_dictionary -s 727369 --minimal --verbose --check --lookup -p $2 -t 1 --external
./build -n $1 -c 3.5 -a 0.94 -e dictionary_dictionary -s 727369 --minimal --verbose --check --lookup -p $2 -t $3 --external