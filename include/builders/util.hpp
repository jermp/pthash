#pragma once

#include "../utils/util.hpp"

namespace pthash {

struct seed_runtime_error : public std::runtime_error {
    seed_runtime_error() : std::runtime_error("seed did not work") {}
};

struct build_timings {
    build_timings()
        : partitioning_seconds(0.0)
        , mapping_ordering_seconds(0.0)
        , searching_seconds(0.0)
        , encoding_seconds(0.0) {}

    double partitioning_seconds;
    double mapping_ordering_seconds;
    double searching_seconds;
    double encoding_seconds;
};

struct build_configuration {
    build_configuration()
        : c(4.5)
        , alpha(0.98)
        , num_partitions(1)
        , num_buckets(constants::invalid_num_buckets)
        , num_threads(1)
        , seed(constants::invalid_seed)
        , verbose_output(true)
        , tmp_dir(constants::default_tmp_dirname) {}

    double c;
    double alpha;
    uint64_t num_partitions;
    uint64_t num_buckets;
    uint64_t num_threads;
    uint64_t seed;
    bool verbose_output;
    std::string tmp_dir;
};

}  // namespace pthash