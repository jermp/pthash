# /usr/bin/bash

# num_keys: $1

configs=(
  "3.79649 0.97 R-R"
  "3.79649 0.99 C-C"
  "2.41595 0.88 D-D"
  "4.42924 0.99  EF"
  "3.79649 0.94 D-D"
)

# XOR-type search, single thread, single function
for config in "${configs[@]}"; do
  IFS=' ' read -r l a e <<< "$config"
  # run the command 3 times for each configuration
  for i in {1..3}; do
    ./build -n "$1" -l "$l" -a "$a" -e "$e" -r xor -b skew -s 1234567890 -q 10000000 --minimal
  done
done

# XOR-type search, multiple thread, partitioned function
for config in "${configs[@]}"; do
  IFS=' ' read -r l a e <<< "$config"
  # run the command 3 times for each configuration
  for i in {1..3}; do
    ./build -n "$1" -l "$l" -a "$a" -e "$e" -r xor -b skew -s 1234567890 -q 10000000 -p 5000000 -t 8 --minimal
  done
done
