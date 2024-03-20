#pragma once

#include "search_util.hpp"

#include "essentials.hpp"
#include "util.hpp"
#include "encoders/bit_vector.hpp"
#include "utils/hasher.hpp"

namespace pthash {

template <typename BucketsIterator, typename PilotsBuffer>
void search_sequential_xor(const uint64_t num_keys, const uint64_t num_buckets,
                           const uint64_t num_non_empty_buckets, const uint64_t seed,
                           build_configuration const& config, BucketsIterator& buckets,
                           bit_vector_builder& taken, PilotsBuffer& pilots) {
    const uint64_t max_bucket_size = (*buckets).size();
    const uint64_t table_size = taken.size();
    const __uint128_t M = fastmod::computeM_u64(table_size);

    std::vector<uint64_t> positions;
    positions.reserve(max_bucket_size);

    std::vector<uint64_t> hashed_pilots_cache(search_cache_size);
    for (uint64_t pilot = 0; pilot != search_cache_size; ++pilot) {
        hashed_pilots_cache[pilot] = default_hash64(pilot, seed);
    }

    search_logger log(num_keys, num_buckets);
    if (config.verbose_output) log.init();

    uint64_t processed_buckets = 0;
    for (; processed_buckets < num_non_empty_buckets; ++processed_buckets, ++buckets) {
        auto const& bucket = *buckets;
        assert(bucket.size() > 0);

        for (uint64_t pilot = 0; true; ++pilot) {
            uint64_t hashed_pilot = PTHASH_LIKELY(pilot < search_cache_size)
                                        ? hashed_pilots_cache[pilot]
                                        : default_hash64(pilot, seed);

            positions.clear();

            auto bucket_begin = bucket.begin(), bucket_end = bucket.end();
            for (; bucket_begin != bucket_end; ++bucket_begin) {
                uint64_t hash = *bucket_begin;
                uint64_t p = fastmod::fastmod_u64(hash ^ hashed_pilot, M, table_size);
                if (taken.get(p)) break;
                positions.push_back(p);
            }

            if (bucket_begin == bucket_end) {  // all keys do not have collisions with taken

                // check for in-bucket collisions
                std::sort(positions.begin(), positions.end());
                auto it = std::adjacent_find(positions.begin(), positions.end());
                if (it != positions.end())
                    continue;  // in-bucket collision detected, try next pilot

                pilots.emplace_back(bucket.id(), pilot);
                for (auto p : positions) {
                    assert(taken.get(p) == false);
                    taken.set(p, true);
                }
                if (config.verbose_output) log.update(processed_buckets, bucket.size());
                break;
            }
        }
    }

    if (config.verbose_output) log.finalize(processed_buckets);
}

template <typename BucketsIterator, typename PilotsBuffer>
void search_parallel_xor(const uint64_t num_keys, const uint64_t num_buckets,
                         const uint64_t num_non_empty_buckets, const uint64_t seed,
                         build_configuration const& config, BucketsIterator& buckets,
                         bit_vector_builder& taken, PilotsBuffer& pilots) {
    const uint64_t max_bucket_size = (*buckets).size();
    const uint64_t table_size = taken.size();
    const __uint128_t M = fastmod::computeM_u64(table_size);
    const uint64_t num_threads = config.num_threads;

    std::vector<uint64_t> hashed_pilots_cache(search_cache_size);
    for (uint64_t pilot = 0; pilot != search_cache_size; ++pilot) {
        hashed_pilots_cache[pilot] = default_hash64(pilot, seed);
    }

    search_logger log(num_keys, num_buckets);
    if (config.verbose_output) log.init();

    std::atomic<uint64_t> next_bucket_idx = 0;
    static_assert(next_bucket_idx.is_always_lock_free);

    auto exe = [&](uint64_t local_bucket_idx, bucket_t bucket) {
        std::vector<uint64_t> positions;
        positions.reserve(max_bucket_size);

        while (true) {
            uint64_t pilot = 0;
            bool pilot_checked = false;

            while (true) {
                uint64_t local_next_bucket_idx = next_bucket_idx;

                for (; true; ++pilot) {
                    if (PTHASH_LIKELY(!pilot_checked)) {
                        uint64_t hashed_pilot = PTHASH_LIKELY(pilot < search_cache_size)
                                                    ? hashed_pilots_cache[pilot]
                                                    : default_hash64(pilot, seed);

                        positions.clear();

                        auto bucket_begin = bucket.begin(), bucket_end = bucket.end();
                        for (; bucket_begin != bucket_end; ++bucket_begin) {
                            uint64_t hash = *bucket_begin;
                            uint64_t p = fastmod::fastmod_u64(hash ^ hashed_pilot, M, table_size);
                            if (taken.get(p)) break;
                            positions.push_back(p);
                        }

                        if (bucket_begin == bucket_end) {
                            std::sort(positions.begin(), positions.end());
                            auto it = std::adjacent_find(positions.begin(), positions.end());
                            if (it != positions.end()) continue;

                            // I can stop the pilot search as there are not collisions
                            pilot_checked = true;
                            break;
                        }
                    } else {
                        // I already computed the positions and checked the in-bucket collisions
                        // I must only check the bitmap again
                        for (auto p : positions) {
                            if (taken.get(p)) {
                                pilot_checked = false;
                                break;
                            }
                        }
                        // I can stop the pilot search as there are no collisions
                        if (pilot_checked) break;
                    }
                }

                // I am the first thread: this is the only condition that can stop the loop
                if (local_next_bucket_idx == local_bucket_idx) break;

                // active wait until another thread pushes a change in the bitmap
                while (local_next_bucket_idx == next_bucket_idx)
                    ;
            }
            assert(local_bucket_idx == next_bucket_idx);

            /* thread-safe from now on */

            pilots.emplace_back(bucket.id(), pilot);
            for (auto p : positions) {
                assert(taken.get(p) == false);
                taken.set(p, true);
            }
            if (config.verbose_output) log.update(local_bucket_idx, bucket.size());

            // update (local) local_bucket_idx
            local_bucket_idx = next_bucket_idx + num_threads;

            if (local_bucket_idx >= num_non_empty_buckets) {  // stop the thread
                // update (global) next_bucket_idx, which may unlock other threads
                ++next_bucket_idx;
                break;
            }

            // read the next bucket and advance the iterator
            bucket = (*buckets);
            ++buckets;

            // update (global) next_bucket_idx, which may unlock other threads
            ++next_bucket_idx;
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    next_bucket_idx = static_cast<uint64_t>(-1);  // avoid that some thread advances the iterator
    for (uint64_t i = 0; i != num_threads and i < num_non_empty_buckets; ++i, ++buckets) {
        bucket_t bucket = *buckets;
        threads.emplace_back(exe, i, bucket);
    }

    next_bucket_idx = 0;  // notify the first thread
    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }
    assert(next_bucket_idx == num_non_empty_buckets);

    if (config.verbose_output) log.finalize(next_bucket_idx);
}

}  // namespace pthash