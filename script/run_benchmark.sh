# 100M keys
./build 100000000 7.0 0.99 compact_compact -s 1234567890 --minimal --verbose --lookup
./build 100000000 11.0 0.88 dictionary_dictionary -s 1234567890 --minimal --verbose --lookup
./build 100000000 6.0 0.99 elias_fano -s 1234567890 --minimal --verbose --lookup
./build 100000000 7.0 0.94 dictionary_dictionary -s 1234567890 --minimal --verbose --lookup

# 1B keys
./build 1000000000 7.0 0.99 compact_compact -s 1234567890 --minimal --verbose --lookup
./build 1000000000 11.0 0.88 dictionary_dictionary -s 1234567890 --minimal --verbose --lookup
./build 1000000000 6.0 0.99 elias_fano -s 1234567890 --minimal --verbose --lookup
./build 1000000000 7.0 0.94 dictionary_dictionary -s 1234567890 --minimal --verbose --lookup
