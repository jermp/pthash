# 100M keys
./build -n 100000000 -c 7.0 -a 0.99 -e compact_compact -s 1234567890 --minimal --verbose --lookup
./build -n 100000000 -c 11.0 -a 0.88 -e dictionary_dictionary -s 1234567890 --minimal --verbose --lookup
./build -n 100000000 -c 6.0 -a 0.99 -e elias_fano -s 1234567890 --minimal --verbose --lookup
./build -n 100000000 -c 7.0 -a 0.94 -e dictionary_dictionary -s 1234567890 --minimal --verbose --lookup

# 1B keys
./build -n 1000000000 -c 7.0 -a 0.99 -e compact_compact -s 1234567890 --minimal --verbose --lookup
./build -n 1000000000 -c 11.0 -a 0.88 -e dictionary_dictionary -s 1234567890 --minimal --verbose --lookup
./build -n 1000000000 -c 6.0 -a 0.99 -e elias_fano -s 1234567890 --minimal --verbose --lookup
./build -n 1000000000 -c 7.0 -a 0.94 -e dictionary_dictionary -s 1234567890 --minimal --verbose --lookup
