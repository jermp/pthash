#pragma once

#include "search_util.hpp"

#include "essentials.hpp"
#include "util.hpp"

namespace pthash {

template <typename BucketsIterator, typename PilotsBuffer>
void search_sequential_add(const uint64_t num_keys, const uint64_t num_buckets,
                           const uint64_t num_non_empty_buckets, const uint64_t /* seed */,
                           build_configuration const& config, BucketsIterator& buckets,
                           bits::bit_vector::builder& taken, PilotsBuffer& pilots) {
    const uint64_t max_bucket_size = (*buckets).size();
    const uint64_t table_size = taken.num_bits();
    const uint64_t M = fastmod::computeM_u32(table_size);

    std::vector<uint64_t> positions;
    positions.reserve(max_bucket_size);

    search_logger log(num_keys, num_buckets);
    if (config.verbose_output) log.init();

    uint64_t processed_buckets = 0;
    for (; processed_buckets < num_non_empty_buckets; ++processed_buckets, ++buckets) {
        auto const& bucket = *buckets;
        assert(bucket.size() > 0);

        for (uint64_t s = 0; true; ++s)  //
        {
            positions.clear();
            auto bucket_begin = bucket.begin(), bucket_end = bucket.end();
            for (; bucket_begin != bucket_end; ++bucket_begin) {
                uint64_t hash = *bucket_begin;
                uint64_t initial_position =
                    fastmod::fastmod_u32(hash64(hash + s).mix() >> 33, M, table_size);
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
                         const uint64_t num_non_empty_buckets, const uint64_t /* seed */,
                         build_configuration const& config, BucketsIterator& buckets,
                         bits::bit_vector::builder& taken, PilotsBuffer& pilots) {
    const uint64_t max_bucket_size = (*buckets).size();
    const uint64_t table_size = taken.num_bits();
    const uint64_t M = fastmod::computeM_u32(table_size);
    const uint64_t num_threads = config.num_threads;

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

                while (true) {
                    if (PTHASH_LIKELY(!pilot_checked)) {
                        for (uint64_t s = 0; true; ++s)  //
                        {
                            positions.clear();
                            auto bucket_begin = bucket.begin(), bucket_end = bucket.end();
                            for (; bucket_begin != bucket_end; ++bucket_begin) {
                                uint64_t hash = *bucket_begin;
                                uint64_t initial_position = fastmod::fastmod_u32(
                                    hash64(hash + s).mix() >> 33, M, table_size);
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

                                if (i == positions.size()) {
                                    /* all keys do not have collisions with taken */
                                    pilot = s * table_size + d;
                                    pilot_found = true;

                                    // update positions
                                    for (auto& p : positions) p += d;

                                    // I can stop the pilot search as there are no collisions
                                    pilot_checked = true;
                                    break;
                                }
                            }

                            if (pilot_found) break;
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
                        // I can stop the pilot search as there are not collisions
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