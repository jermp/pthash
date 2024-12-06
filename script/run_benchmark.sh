# 100M keys
./build -n 100000000 -l 3.79649 -a 0.99 -e C-C -r xor -b skew -s 1234567890 -q 10000000 --minimal --verbose
./build -n 100000000 -l 2.41595 -a 0.88 -e D-D -r xor -b skew -s 1234567890 -q 10000000 --minimal --verbose
./build -n 100000000 -l 4.42924 -a 0.99 -e  EF -r xor -b skew -s 1234567890 -q 10000000 --minimal --verbose
./build -n 100000000 -l 3.79649 -a 0.94 -e D-D -r xor -b skew -s 1234567890 -q 10000000 --minimal --verbose

./build -n 100000000 -l 3.79649 -a 0.99 -e C-C -r xor -b skew -s 1234567890 -q 10000000 -p 5000000 -t 8 --minimal --verbose
./build -n 100000000 -l 2.41595 -a 0.88 -e D-D -r xor -b skew -s 1234567890 -q 10000000 -p 5000000 -t 8 --minimal --verbose
./build -n 100000000 -l 4.42924 -a 0.99 -e  EF -r xor -b skew -s 1234567890 -q 10000000 -p 5000000 -t 8 --minimal --verbose
./build -n 100000000 -l 3.79649 -a 0.94 -e D-D -r xor -b skew -s 1234567890 -q 10000000 -p 5000000 -t 8 --minimal --verbose

./build -n 100000000 -l 3.79649 -a 0.99 -e inter-C  -r add -b skew -s 1234567890 -q 10000000 -p 3000 -t 8 --dense --minimal --verbose
./build -n 100000000 -l 2.41595 -a 0.88 -e inter-D  -r add -b skew -s 1234567890 -q 10000000 -p 3000 -t 8 --dense --minimal --verbose
./build -n 100000000 -l 4.42924 -a 0.99 -e inter-EF -r add -b skew -s 1234567890 -q 10000000 -p 3000 -t 8 --dense --minimal --verbose
./build -n 100000000 -l 3.79649 -a 0.94 -e inter-D  -r add -b skew -s 1234567890 -q 10000000 -p 3000 -t 8 --dense --minimal --verbose

# 1B keys
# ./build -n 1000000000 -l 3.79649 -a 0.99 -e C-C -r xor -b skew -s 1234567890 -q 10000000 --minimal --verbose
# ./build -n 1000000000 -l 2.41595 -a 0.88 -e D-D -r xor -b skew -s 1234567890 -q 10000000 --minimal --verbose
# ./build -n 1000000000 -l 4.42924 -a 0.99 -e  EF -r xor -b skew -s 1234567890 -q 10000000 --minimal --verbose
# ./build -n 1000000000 -l 3.79649 -a 0.94 -e D-D -r xor -b skew -s 1234567890 -q 10000000 --minimal --verbose
