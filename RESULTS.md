# Preliminary results

All results obtained with an Intel(R) Core(TM) i9-9900K CPU @ 3.60GHz processor.

    ./build -n 10000000 -l 9.5 -a 1.00 -e R-R -b skew -t 16 -s 0 --verbose
    ./build -n 10000000 -l 9.5 -a 1.00 -e R-R -b skew -p 1000 -t 16 -s 0 --verbose --lookup --check
    ./build -n 10000000 -l 9.5 -a 1.00 -e mono-R -b skew -p 1000 -t 16 -s 0 --dense --verbose --lookup --check

    ./build -n 10000000 -l 9.5 -a 1.00 -e R-R -b opt1 -t 16 -s 0 --verbose
    ./build -n 10000000 -l 9.5 -a 1.00 -e R-R -b opt1 -p 1000 -t 16 -s 0 --verbose --lookup --check
    ./build -n 10000000 -l 9.5 -a 1.00 -e mono-R -b opt1 -p 1000 -t 16 -s 0 --dense --verbose --lookup --check
