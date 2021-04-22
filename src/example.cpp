#include <iostream>

#include "../include/pthash.hpp"
#include "util.hpp"  // for functions distinct_keys and check

int main() {
    using namespace pthash;

    /* Generate 10M random 64-bit keys as input data. */
    static const uint64_t num_keys = 10000000;
    static const uint64_t seed = 1234567890;
    std::cout << "generating input data..." << std::endl;
    std::vector<uint64_t> keys = distinct_keys<uint64_t>(num_keys, seed);
    assert(keys.size() == num_keys);

    /* Set up a build configuration. */
    build_configuration config;
    config.c = 6.0;
    config.alpha = 0.94;
    config.verbose_output = true;

    /* Declare the PTHash function. */
    typedef single_mphf<murmurhash2_64,        // base hasher
                        dictionary_dictionary  // encoder type
                        >
        pthash_type;
    pthash_type f;

    /* Build the function in internal memory. */
    std::cout << "building the MPHF..." << std::endl;
    f.build_in_internal_memory(keys.begin(), keys.size(), config);

    /* Compute and print the number of bits spent per key. */
    double bits_per_key = static_cast<double>(f.num_bits()) / f.num_keys();
    std::cout << "MPHF uses " << bits_per_key << " [bits/key]" << std::endl;

    /* Sanity check! */
    if (check(keys.begin(), keys.size(), f)) std::cout << "EVERYTHING OK!" << std::endl;

    /* Now evaluate f on some keys. */
    for (uint64_t i = 0; i != 10; ++i) {
        std::cout << "f(" << keys[i] << ") = " << f(keys[i]) << '\n';
    }

    /* Serialize the data structure to a file. */
    std::cout << "serializing the MPHF to disk..." << std::endl;
    std::string output_filename("mphf.bin");
    essentials::save(f, output_filename.c_str());

    /* Now reload from disk and query. */
    pthash_type other;
    essentials::load(other, output_filename.c_str());
    for (uint64_t i = 0; i != 10; ++i) {
        std::cout << "f(" << keys[i] << ") = " << other(keys[i]) << '\n';
        assert(f(keys[i]) == other(keys[i]));
    }

    return 0;
}