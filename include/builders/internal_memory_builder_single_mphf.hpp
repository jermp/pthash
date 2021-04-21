#pragma once

#include "util.hpp"
#include "../utils/bucketers.hpp"
#include "../utils/logger.hpp"
#include "../utils/fill_free_slots.hpp"
#include "../utils/hasher.hpp"

namespace pthash {

template <typename Hasher>
struct internal_memory_builder_single_mphf {
    typedef Hasher hasher_type;

    struct bucket {
        typedef std::vector<uint64_t>::const_iterator const_iterator;

        bucket() {}

        bucket(uint64_t id, const_iterator begin, uint64_t size)
            : m_id(id), m_begin(begin), m_end(begin + size) {}

        inline uint64_t id() const {
            return m_id;
        }

        inline const_iterator begin() const {
            return m_begin;
        }

        inline const_iterator end() const {
            return m_end;
        }

        inline uint64_t size() const {
            return std::distance(m_begin, m_end);
        }

    private:
        uint64_t m_id;
        const_iterator m_begin;
        const_iterator m_end;
    };

    template <typename Iterator>
    build_timings build_from_keys(Iterator keys, uint64_t num_keys,
                                  build_configuration const& config) {
        if (config.seed == constants::invalid_seed) {
            for (auto attempt = 0; attempt < 10; ++attempt) {
                m_seed = random_value();
                try {
                    return build_from_hashes(hash_iterator<Iterator>(keys, m_seed), num_keys,
                                             config);
                } catch (seed_runtime_error const& error) {
                    std::cout << "attempt " << attempt + 1 << " failed" << std::endl;
                }
            }
            throw seed_runtime_error();
        }
        m_seed = config.seed;
        return build_from_hashes(hash_iterator<Iterator>(keys, m_seed), num_keys, config);
    }

    template <typename Iterator>
    build_timings build_from_hashes(Iterator hashes, uint64_t num_keys,
                                    build_configuration const& config) {
        if (config.alpha == 0 or config.alpha > 1.0) {
            throw std::invalid_argument("load factor must be > 0 and <= 1.0");
        }

        clock_type::time_point start, stop;

        start = clock_type::now();

        build_timings time;

        uint64_t table_size = static_cast<double>(num_keys) / config.alpha;
        if ((table_size & (table_size - 1)) == 0) table_size += 1;
        uint64_t num_buckets = (config.num_buckets == constants::invalid_num_buckets)
                                   ? (std::ceil((config.c * num_keys) / std::log2(num_keys)))
                                   : config.num_buckets;

        m_num_keys = num_keys;
        m_table_size = table_size;

        if (config.verbose_output) {
            std::cout << "c = " << config.c << std::endl;
            std::cout << "alpha = " << config.alpha << std::endl;
            std::cout << "num_keys = " << num_keys << std::endl;
            std::cout << "table_size = " << table_size << std::endl;
            std::cout << "num_buckets = " << num_buckets << std::endl;
        }

        std::vector<bucket> buckets;
        buckets.reserve(num_buckets);
        uint64_t max_bucket_size = 0;
        std::vector<uint64_t> sorted_hashes(num_keys + 1);
        {
            typedef std::pair<uint64_t, uint64_t> record;  // (bucket_id, hash)

            auto tmp = clock_type::now();
            std::vector<record> records(num_keys + 1);
            m_bucketer.init(num_buckets);
            Iterator begin = hashes;
            for (uint64_t i = 0; i != num_keys; ++i, ++begin) {
                auto hash = *begin;
                auto bucket_id = m_bucketer.bucket(hash.first());
                records[i] = {bucket_id, hash.second()};
            }
            records[num_keys] = {uint64_t(-1), uint64_t(-1)};  // dummy record
            std::sort(records.begin(), records.end(), [](record const& a, record const& b) {
                return (a.first < b.first) or (a.first == b.first and a.second < b.second);
            });
            if (config.verbose_output) {
                std::cout << " == mapping took: " << seconds(clock_type::now() - tmp) << " seconds"
                          << std::endl;
            }

            tmp = clock_type::now();
            bool collision_found = false;
            sorted_hashes[0] = records[0].second;
            uint64_t prev_size = 1;
            for (uint64_t i = 1; i != records.size(); ++i) {
                sorted_hashes[i] = records[i].second;
                if (records[i].first == records[i - 1].first) {
                    if (records[i].second != records[i - 1].second) {
                        ++prev_size;
                    } else {
                        collision_found = true;
                        break;
                    }
                } else {
                    buckets.emplace_back(records[i - 1].first,
                                         sorted_hashes.cbegin() + i - prev_size, prev_size);
                    if (prev_size > max_bucket_size) max_bucket_size = prev_size;
                    prev_size = 1;
                }
            }

            if (config.verbose_output) {
                std::cout << " == check took: " << seconds(clock_type::now() - tmp) << " seconds"
                          << std::endl;
            }
            if (collision_found) throw seed_runtime_error();
        }

        {
            std::vector<bucket> buckets_ordered(buckets.size());
            std::vector<uint64_t> offsets(max_bucket_size + 1, 0);
            for (auto const& bucket : buckets) ++offsets[bucket.size() - 1];
            for (uint64_t i = max_bucket_size; i > 0; --i) offsets[i - 1] += offsets[i];
            for (auto const& bucket : buckets) buckets_ordered[offsets[bucket.size()]++] = bucket;
            buckets = std::move(buckets_ordered);
        }

        stop = clock_type::now();
        time.mapping_ordering_seconds = seconds(stop - start);
        if (config.verbose_output) {
            std::cout << " == map+sort took " << time.mapping_ordering_seconds << " seconds "
                      << std::endl;
            std::cout << " == max bucket size = " << max_bucket_size << std::endl;
        }

        start = clock_type::now();
        bit_vector_builder taken(table_size);
        search(taken, num_buckets, buckets, config);
        fill_free_slots(taken, num_keys, m_free_slots);
        stop = clock_type::now();
        time.searching_seconds = seconds(stop - start);
        if (config.verbose_output) {
            std::cout << " == search took " << time.searching_seconds << " seconds" << std::endl;
        }

        return time;
    }

    uint64_t seed() const {
        return m_seed;
    }

    uint64_t num_keys() const {
        return m_num_keys;
    }

    uint64_t table_size() const {
        return m_table_size;
    }

    skew_bucketer bucketer() const {
        return m_bucketer;
    }

    std::vector<uint64_t> const& pilots() const {
        return m_pilots;
    }

    std::vector<uint64_t> const& free_slots() const {
        return m_free_slots;
    }

private:
    uint64_t m_seed;
    uint64_t m_num_keys;
    uint64_t m_table_size;
    skew_bucketer m_bucketer;
    std::vector<uint64_t> m_pilots;
    std::vector<uint64_t> m_free_slots;

    template <typename Iterator>
    struct hash_iterator {
        hash_iterator(Iterator keys, uint64_t seed) : m_iterator(keys), m_seed(seed) {}

        inline auto operator*() {
            return hasher_type::hash(*m_iterator, m_seed);
        }

        inline void operator++() {
            ++m_iterator;
        }

    private:
        Iterator m_iterator;
        uint64_t m_seed;
    };

    void search(bit_vector_builder& taken, uint64_t num_buckets, std::vector<bucket> const& buckets,
                build_configuration const& config) {
        uint64_t max_bucket_size = buckets.front().size();
        uint64_t table_size = taken.size();
        std::vector<uint64_t> local;
        local.reserve(max_bucket_size);
        m_pilots.resize(num_buckets, 0);
        __uint128_t M = fastmod::computeM_u64(table_size);

        constexpr uint64_t cache_size = 1000;
        std::vector<uint64_t> hashed_pilots_cache(cache_size);
        for (uint64_t pilot = 0; pilot != cache_size; ++pilot) {
            hashed_pilots_cache[pilot] = default_hash64(pilot, m_seed);
        }

        logger log(m_num_keys, table_size, num_buckets);

        for (uint64_t i = 0; i != buckets.size(); ++i) {
            auto const& bucket = buckets[i];
            assert(bucket.size() > 0);

            uint64_t pilot = 0;
            for (; true; ++pilot) {
                uint64_t hashed_pilot = PTH_LIKELY(pilot < cache_size)
                                            ? hashed_pilots_cache[pilot]
                                            : default_hash64(pilot, m_seed);

                auto bucket_it = bucket.begin(), bucket_end = bucket.end();
                for (; bucket_it != bucket_end; ++bucket_it) {
                    uint64_t hash = *bucket_it;
                    uint64_t p = fastmod::fastmod_u64(hash ^ hashed_pilot, M, table_size);
                    if (taken.get(p)) {
                        local.clear();
                        break;
                    }
                    local.push_back(p);
                }

                if (bucket_it == bucket_end) {  // all keys do not have collisions with taken

                    // check for in-bucket collisions
                    std::sort(local.begin(), local.end());
                    auto it = std::adjacent_find(local.begin(), local.end());
                    if (it != local.end()) {  // in-bucket collision detected
                        local.clear();
                        continue;  // try the next pilot
                    }

                    m_pilots[bucket.id()] = pilot;

                    for (auto p : local) {
                        assert(taken.get(p) == false);
                        taken.set(p, true);
                    }
                    local.clear();
                    break;
                }
            }

            if (config.verbose_output) log.update(i, bucket.size(), pilot);
        }

        if (config.verbose_output) log.finalize(buckets.size());
    }
};

}  // namespace pthash