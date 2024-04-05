#include <iostream>

#include "pthash.hpp"
#include "util.hpp"  // for functions distinct_keys and check

int main() {
    using namespace pthash;

    /* Generate 1M random 64-bit keys as input data. */
    static const uint64_t num_keys = 1000000;
    static const uint64_t seed = 1234567890;
    std::cout << "generating input data..." << std::endl;
    std::vector<uint64_t> keys = distinct_keys<uint64_t>(num_keys, default_hash64(seed, seed));
    assert(keys.size() == num_keys);

    /* Set up a build configuration. */
    build_configuration config;
    config.seed = seed;
    config.lambda = 5;
    config.alpha = 1;
    config.num_threads = 8;
    config.search = pthash_search_type::add_displacement;
    config.dense_partitioning = true;
    config.avg_partition_size = 2048;
    config.minimal_output = true;  // mphf
    config.verbose_output = true;
    config.secondary_sort = true;

    /* Declare the PTHash function. */
    // typedef single_phf<
    //     murmurhash2_64,                                                       // base hasher
    //     skew_bucketer,                                                        // bucketer type
    //     dictionary_dictionary,                                                // encoder type
    //     true,                                                                 // minimal
    //     pthash_search_type::add_displacement                                  // xor displacement
    //                                                                           // search
    //     >
    typedef dense_partitioned_phf<murmurhash2_64,                       // base hasher
                                  table_bucketer<opt_bucketer>,                         // bucketer type
                                  multi_C,                              // encoder type
                                  false,                                 // minimal
                                  pthash_search_type::add_displacement  // additive
                                                                        // displacement search
                                  >
        pthash_type;

    pthash_type f;

    /* Build the function in internal memory. */
    std::cout << "building the function..." << std::endl;
    auto start = clock_type::now();
    auto timings = f.build_in_internal_memory(keys.begin(), keys.size(), config);
    double total_microseconds = timings.partitioning_microseconds +
                                timings.mapping_ordering_microseconds +
                                timings.searching_microseconds + timings.encoding_microseconds;
    std::cout << "function built in " << to_microseconds(clock_type::now() - start) / 1000000
              << " seconds" << std::endl;
    std::cout << "computed: " << total_microseconds / 1000000 << " seconds" << std::endl;
    /* Compute and print the number of bits spent per key. */
    double bits_per_key = static_cast<double>(f.num_bits()) / f.num_keys();
    std::cout << "function uses " << bits_per_key << " [bits/key]" << std::endl;



    std::cout << "part" << timings.partitioning_microseconds  << std::endl;
    /* Sanity check! */
    if (check(keys.begin(), f)) std::cout << "EVERYTHING OK!" << std::endl;

    return 0;
}