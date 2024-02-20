# Preliminary results

All results obtained with an Intel(R) Core(TM) i9-9900K CPU @ 3.60GHz processor.

    ./build -n 1000000 -c 2.0 -a 1.0 -e partitioned_compact -b skew --minimal --verbose

    {"n": "1000000", "c": "2.000000", "alpha": "1.000000", "minimal": "true", "encoder_type": "partitioned_compact", "num_partitions": "1", "num_threads": "1", "external_memory": "false", "partitioning_seconds": "0.000000", "mapping_ordering_seconds": "0.089000", "searching_seconds": "59.091000", "encoding_seconds": "0.000000", "total_seconds": "59.180000", "pt_bits_per_key": "2.009120", "mapper_bits_per_key": "0.000640", "bits_per_key": "2.009760", "nanosec_per_key": "0.000000"}

    ./build -n 1000000 -c 2.0 -a 1.0 -e partitioned_compact -b opt --minimal --verbose

    {"n": "1000000", "c": "2.000000", "alpha": "1.000000", "minimal": "true", "encoder_type": "partitioned_compact", "num_partitions": "1", "num_threads": "1", "external_memory": "false", "partitioning_seconds": "0.000000", "mapping_ordering_seconds": "0.097000", "searching_seconds": "38.851000", "encoding_seconds": "0.000000", "total_seconds": "38.948000", "pt_bits_per_key": "1.908192", "mapper_bits_per_key": "0.000640", "bits_per_key": "1.908832", "nanosec_per_key": "0.000000"}




    ./build -n 10000000 -c 3.0 -a 1.0 -e partitioned_compact -b skew --minimal --verbose

    {"n": "10000000", "c": "3.000000", "alpha": "1.000000", "minimal": "true", "encoder_type": "partitioned_compact", "num_partitions": "1", "num_threads": "1", "external_memory": "false", "partitioning_seconds": "0.000000", "mapping_ordering_seconds": "0.974000", "searching_seconds": "72.748000", "encoding_seconds": "0.003000", "total_seconds": "73.725000", "pt_bits_per_key": "2.068118", "mapper_bits_per_key": "0.000064", "bits_per_key": "2.068182", "nanosec_per_key": "0.000000"}

    ./build -n 10000000 -c 3.0 -a 1.0 -e partitioned_compact -b opt --minimal --verbose

    {"n": "10000000", "c": "3.000000", "alpha": "1.000000", "minimal": "true", "encoder_type": "partitioned_compact", "num_partitions": "1", "num_threads": "1", "external_memory": "false", "partitioning_seconds": "0.000000", "mapping_ordering_seconds": "1.061000", "searching_seconds": "67.786000", "encoding_seconds": "0.003000", "total_seconds": "68.850000", "pt_bits_per_key": "2.039325", "mapper_bits_per_key": "0.000064", "bits_per_key": "2.039389", "nanosec_per_key": "0.000000"}



    ./build -n 10000000 -c 5.0 -a 0.97 -e partitioned_compact -b skew --minimal --verbose

    {"n": "10000000", "c": "5.000000", "alpha": "0.970000", "minimal": "true", "encoder_type": "partitioned_compact", "num_partitions": "1", "num_threads": "1", "external_memory": "false", "partitioning_seconds": "0.000000", "mapping_ordering_seconds": "0.983000", "searching_seconds": "2.698000", "encoding_seconds": "0.006000", "total_seconds": "3.687000", "pt_bits_per_key": "2.250883", "mapper_bits_per_key": "0.234299", "bits_per_key": "2.485182", "nanosec_per_key": "0.000000"}

    ./build -n 10000000 -c 5.0 -a 0.97 -e partitioned_compact -b opt --minimal --verbose

    {"n": "10000000", "c": "5.000000", "alpha": "0.970000", "minimal": "true", "encoder_type": "partitioned_compact", "num_partitions": "1", "num_threads": "1", "external_memory": "false", "partitioning_seconds": "0.000000", "mapping_ordering_seconds": "1.060000", "searching_seconds": "3.097000", "encoding_seconds": "0.006000", "total_seconds": "4.163000", "pt_bits_per_key": "2.305501", "mapper_bits_per_key": "0.234299", "bits_per_key": "2.539800", "nanosec_per_key": "0.000000"}



    ./build -n 10000000 -c 2.6 -a 0.97 -e partitioned_compact -b skew --minimal --verbose -t 8

    {"n": "10000000", "c": "2.600000", "alpha": "0.970000", "minimal": "true", "encoder_type": "partitioned_compact", "num_partitions": "1", "num_threads": "8", "external_memory": "false", "partitioning_seconds": "0.000000", "mapping_ordering_seconds": "0.403000", "searching_seconds": "36.043000", "encoding_seconds": "0.004000", "total_seconds": "36.450000", "pt_bits_per_key": "1.869142", "mapper_bits_per_key": "0.234299", "bits_per_key": "2.103442", "nanosec_per_key": "0.000000"}

    ./build -n 10000000 -c 2.6 -a 0.97 -e partitioned_compact -b opt --minimal --verbose -t 8

    {"n": "10000000", "c": "2.600000", "alpha": "0.970000", "minimal": "true", "encoder_type": "partitioned_compact", "num_partitions": "1", "num_threads": "8", "external_memory": "false", "partitioning_seconds": "0.000000", "mapping_ordering_seconds": "0.416000", "searching_seconds": "34.888000", "encoding_seconds": "0.004000", "total_seconds": "35.308000", "pt_bits_per_key": "1.848349", "mapper_bits_per_key": "0.234299", "bits_per_key": "2.082648", "nanosec_per_key": "0.000000"}



    ./build -n 10000000 -c 2.6 -a 1.0 -e partitioned_compact -b skew --minimal --verbose -t 8

    {"n": "10000000", "c": "2.600000", "alpha": "1.000000", "minimal": "true", "encoder_type": "partitioned_compact", "num_partitions": "1", "num_threads": "8", "external_memory": "false", "partitioning_seconds": "0.000000", "mapping_ordering_seconds": "0.397000", "searching_seconds": "83.667000", "encoding_seconds": "0.003000", "total_seconds": "84.067000", "pt_bits_per_key": "2.028374", "mapper_bits_per_key": "0.000064", "bits_per_key": "2.028438", "nanosec_per_key": "0.000000"}

    ./build -n 10000000 -c 2.6 -a 1.0 -e partitioned_compact -b opt --minimal --verbose -t 8

    {"n": "10000000", "c": "2.600000", "alpha": "1.000000", "minimal": "true", "encoder_type": "partitioned_compact", "num_partitions": "1", "num_threads": "8", "external_memory": "false", "partitioning_seconds": "0.000000", "mapping_ordering_seconds": "0.388000", "searching_seconds": "66.392000", "encoding_seconds": "0.002000", "total_seconds": "66.782000", "pt_bits_per_key": "1.959171", "mapper_bits_per_key": "0.000064", "bits_per_key": "1.959235", "nanosec_per_key": "0.000000"}
