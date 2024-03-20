#pragma once

#include "search_util.hpp"

#include "essentials.hpp"
#include "util.hpp"
#include "encoders/bit_vector.hpp"
#include "utils/hasher.hpp"

namespace pthash {

template <typename BucketsIterator, typename PilotsBuffer>
void search_sequential_add(const uint64_t num_keys, const uint64_t num_buckets,
                           const uint64_t num_non_empty_buckets, const uint64_t seed,
                           build_configuration const& config, BucketsIterator& buckets,
                           bit_vector_builder& taken, PilotsBuffer& pilots) {
    const uint64_t max_bucket_size = (*buckets).size();
    const uint64_t table_size = taken.size();
    std::vector<uint64_t> positions;
    positions.reserve(max_bucket_size);
    const __uint128_t M = fastmod::computeM_u64(table_size);

    std::vector<uint64_t> hashed_seed_cache(search_cache_size);
    for (uint64_t s = 0; s != search_cache_size; ++s) {
        hashed_seed_cache[s] = default_hash64(s, seed);
    }

    search_logger log(num_keys, num_buckets);
    if (config.verbose_output) log.init();

    uint64_t processed_buckets = 0;
    for (; processed_buckets < num_non_empty_buckets; ++processed_buckets, ++buckets) {
        auto const& bucket = *buckets;
        assert(bucket.size() > 0);

        for (uint64_t s = 0; true; ++s)  //
        {
            positions.clear();
            uint64_t hashed_s = PTHASH_LIKELY(s < search_cache_size) ? hashed_seed_cache[s]
                                                                     : default_hash64(s, seed);

            auto bucket_begin = bucket.begin(), bucket_end = bucket.end();
            for (; bucket_begin != bucket_end; ++bucket_begin) {
                uint64_t hash = *bucket_begin;
                uint64_t initial_position = fastmod::fastmod_u64(hash ^ hashed_s, M, table_size);
                positions.push_back(initial_position);
            }

            // check for in-bucket collisions
            std::sort(positions.begin(), positions.end());
            auto it = std::adjacent_find(positions.begin(), positions.end());
            if (it != positions.end()) continue;

            bool pilot_found = false;
            for (uint64_t d = 0; d != table_size; ++d)  //
            {
                uint64_t i = 0;
                for (; i != positions.size(); ++i) {
                    uint64_t initial_position = positions[i];
                    uint64_t final_position = initial_position + d;
                    if (final_position >= table_size) {
                        final_position -= table_size;
                        positions[i] -= table_size;
                    }
                    if (taken.get(final_position)) break;
                }

                if (i == positions.size()) {  // all keys do not have collisions with taken
                    const uint64_t pilot = s * table_size + d;
                    pilots.emplace_back(bucket.id(), pilot);
                    for (auto initial_position : positions) {
                        uint64_t final_position = initial_position + d;
                        assert(taken.get(final_position) == false);
                        taken.set(final_position, true);
                    }
                    if (config.verbose_output) log.update(processed_buckets, bucket.size());
                    pilot_found = true;
                    break;
                }
            }

            if (pilot_found) break;
        }
    }

    if (config.verbose_output) log.finalize(processed_buckets);
}

template <typename BucketsIterator, typename PilotsBuffer>
void search_parallel_add(const uint64_t num_keys, const uint64_t num_buckets,
                         const uint64_t num_non_empty_buckets, const uint64_t seed,
                         build_configuration const& config, BucketsIterator& buckets,
                         bit_vector_builder& taken, PilotsBuffer& pilots) {
    assert(false);  // TODO
}

}  // namespace pthash