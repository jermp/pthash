#include <iostream>

#include "include/pthash.hpp"
#include "src/util.hpp"

int main() {
    using namespace pthash;

    /* Generate 1M random 64-bit keys as input data. */
    static const uint64_t num_keys = 1000000;
    static const uint64_t seed = 1234567890;
    std::cout << "generating input data..." << std::endl;
    std::vector<uint64_t> keys = distinct_uints<uint64_t>(num_keys, seed);
    assert(keys.size() == num_keys);

    /* Set up a build configuration. */
    build_configuration config;
    config.seed = seed;
    config.lambda = 5;
    config.alpha = 0.97;
    config.verbose = true;
    config.avg_partition_size = 2000;
    config.dense_partitioning = true;

    /* Declare the PTHash function type. */

    // typedef single_phf<xxhash128,                            // base hasher
    //                    skew_bucketer,                        // bucketer type
    //                    dictionary_dictionary,                // encoder type
    //                    true,                                 // minimal
    //                    pthash_search_type::xor_displacement  // additive displacement
    //                    >
    //     pthash_type;

    // typedef partitioned_phf<xxhash128,                            // base hasher
    //                         opt_bucketer,                         // bucketer
    //                         dictionary_dictionary,                // encoder type
    //                         true,                                 // minimal
    //                         pthash_search_type::add_displacement  // additive displacement
    //                         >
    //     pthash_type;

    typedef dense_partitioned_phf<xxhash128,                            // base hasher
                                  opt_bucketer,                         // bucketer
                                  inter_R,                              // encoder type
                                  true,                                 // minimal
                                  pthash_search_type::add_displacement  // additive displacement
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

    /* Sanity check! */
    if (check(keys.begin(), f)) std::cout << "EVERYTHING OK!" << std::endl;

    /* Now evaluate f on some keys. */
    const uint64_t n = std::min<uint64_t>(10, keys.size());
    for (uint64_t i = 0; i != n; ++i) {
        std::cout << "f(" << keys[i] << ") = " << f(keys[i]) << '\n';
    }

    /* Serialize the data structure to a file. */
    std::cout << "serializing the function to disk..." << std::endl;
    std::string output_filename("pthash.bin");
    essentials::save(f, output_filename.c_str());

    {
        /* Now reload from disk and query. */
        pthash_type other;
        essentials::load(other, output_filename.c_str());
        const uint64_t n = std::min<uint64_t>(10, keys.size());
        for (uint64_t i = 0; i != n; ++i) {
            std::cout << "f(" << keys[i] << ") = " << other(keys[i]) << '\n';
            assert(f(keys[i]) == other(keys[i]));
        }
    }

    std::remove(output_filename.c_str());
    return 0;
}