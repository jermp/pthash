#pragma once

#include <math.h>   // for pow, round, log2
#include <sstream>  // for stringbuf
#include <atomic>   // for std::atomic
#include <vector>

#include "external/bits/include/bit_vector.hpp"

#include "include/builders/util.hpp"
#include "include/utils/hasher.hpp"

#include "search_xor.hpp"
#include "search_add.hpp"

namespace pthash {

template <typename BucketsIterator, typename PilotsBuffer>
void search(const uint64_t num_keys, const uint64_t num_buckets,
            const uint64_t num_non_empty_buckets, const uint64_t seed,
            build_configuration const& config, BucketsIterator& buckets, bits::bit_vector::builder& taken,
            PilotsBuffer& pilots) {
    if (config.num_threads > 1) {
        if (config.num_threads > std::thread::hardware_concurrency()) {
            throw std::invalid_argument("parallel search should use at most " +
                                        std::to_string(std::thread::hardware_concurrency()) +
                                        " threads");
        }
        if (config.search == pthash_search_type::xor_displacement) {
            search_parallel_xor(num_keys, num_buckets, num_non_empty_buckets, seed, config, buckets,
                                taken, pilots);
        } else if (config.search == pthash_search_type::add_displacement) {
            search_parallel_add(num_keys, num_buckets, num_non_empty_buckets, seed, config, buckets,
                                taken, pilots);
        } else {
            assert(false);
        }
    } else {
        if (config.search == pthash_search_type::xor_displacement) {
            search_sequential_xor(num_keys, num_buckets, num_non_empty_buckets, seed, config,
                                  buckets, taken, pilots);
        } else if (config.search == pthash_search_type::add_displacement) {
            search_sequential_add(num_keys, num_buckets, num_non_empty_buckets, seed, config,
                                  buckets, taken, pilots);
        } else {
            assert(false);
        }
    }
}

}  // namespace pthash