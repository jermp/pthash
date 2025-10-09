#pragma once

#include "builders/util.hpp"
#include "builders/search.hpp"
#include "utils/bucketers.hpp"
#include "utils/logger.hpp"
#include "utils/hasher.hpp"

namespace pthash {

template <typename Hasher, typename Bucketer>
struct internal_memory_builder_single_phf {
    typedef Hasher hasher_type;
    typedef Bucketer bucketer_type;

    internal_memory_builder_single_phf()
        : m_seed(constants::invalid_seed)
        , m_num_keys(0)
        , m_num_buckets(0)
        , m_table_size(0)
        , m_bucketer()
        , m_pilots()
        , m_free_slots() {}

    template <typename RandomAccessIterator>
    build_timings build_from_keys(RandomAccessIterator keys, const uint64_t num_keys,
                                  build_configuration const& config)  //
    {
        if (config.seed == constants::invalid_seed) {
            build_configuration actual_config = config;
            for (auto attempt = 0; attempt < 10; ++attempt) {
                actual_config.seed = random_value();
                try {
                    return build_from_hashes(
                        hash_generator<RandomAccessIterator>(keys, actual_config.seed), num_keys,
                        actual_config);
                } catch (seed_runtime_error const& error) {
                    std::cout << "attempt " << attempt + 1 << " failed" << std::endl;
                }
            }
            throw seed_runtime_error();
        }
        return build_from_hashes(hash_generator<RandomAccessIterator>(keys, config.seed), num_keys,
                                 config);
    }

    template <typename RandomAccessIterator>
    build_timings build_from_hashes(RandomAccessIterator hashes, const uint64_t num_keys,
                                    build_configuration const& config)  //
    {
        assert(num_keys > 0);
        util::check_hash_collision_probability<Hasher>(num_keys);

        if (config.alpha == 0 or config.alpha > 1.0) {
            throw std::invalid_argument("load factor must be > 0 and <= 1.0");
        }

        clock_type::time_point start;

        start = clock_type::now();

        build_timings time;

        uint64_t table_size = static_cast<double>(num_keys) / config.alpha;
        if (config.table_size != constants::invalid_table_size) table_size = config.table_size;
        assert(table_size >= num_keys);

        const uint64_t num_buckets = (config.num_buckets == constants::invalid_num_buckets)
                                         ? compute_num_buckets(num_keys, config.lambda)
                                         : config.num_buckets;

        m_seed = config.seed;
        m_num_keys = num_keys;
        m_table_size = table_size;
        m_num_buckets = num_buckets;
        m_bucketer.init(m_num_buckets);

        if (config.verbose) {
            std::cout << "lambda (avg. bucket size) = " << config.lambda << std::endl;
            std::cout << "alpha (load factor) = " << config.alpha << std::endl;
            std::cout << "num_keys = " << num_keys << std::endl;
            std::cout << "table_size = " << table_size << std::endl;
            std::cout << "num_buckets = " << num_buckets << std::endl;
        }

        buckets_t buckets;
        {
            auto start = clock_type::now();
            std::vector<pairs_t> pairs_blocks;
            map(hashes, num_keys, pairs_blocks, config);
            auto elapsed = to_microseconds(clock_type::now() - start);
            if (config.verbose) {
                std::cout << " == map+sort took: " << elapsed / 1'000'000 << " seconds"
                          << std::endl;
            }

            start = clock_type::now();
            merge(pairs_blocks, buckets, config.verbose);
            elapsed = to_microseconds(clock_type::now() - start);
            if (config.verbose) {
                std::cout << " == merge+check took: " << elapsed / 1'000'000 << " seconds"
                          << std::endl;
            }
        }

        auto buckets_iterator = buckets.begin();
        time.mapping_ordering_microseconds = to_microseconds(clock_type::now() - start);
        if (config.verbose) {
            std::cout << " == mapping+ordering took "
                      << time.mapping_ordering_microseconds / 1'000'000 << " seconds " << std::endl;
            buckets.print_bucket_size_distribution();
        }

        start = clock_type::now();
        {
            m_pilots.resize(num_buckets);
            std::fill(m_pilots.begin(), m_pilots.end(), 0);
            bits::bit_vector::builder taken_bvb(m_table_size);
            uint64_t num_non_empty_buckets = buckets.num_buckets();
            pilots_wrapper_t pilots_wrapper(m_pilots);
            search(m_num_keys, m_num_buckets, num_non_empty_buckets,  //
                   config, buckets_iterator, taken_bvb, pilots_wrapper);
            taken_bvb.build(m_taken);
            if (config.minimal) {
                m_free_slots.clear();
                assert(m_taken.num_bits() >= num_keys);
                m_free_slots.reserve(m_taken.num_bits() - num_keys);
                fill_free_slots(m_taken, num_keys, m_free_slots, table_size);
            }
        }
        time.searching_microseconds = to_microseconds(clock_type::now() - start);
        if (config.verbose) {
            std::cout << " == search took " << time.searching_microseconds / 1'000'000 << " seconds"
                      << std::endl;
        }

        return time;
    }

    void set_seed(const uint64_t seed) {
        m_seed = seed;
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

    uint64_t num_partitions() const {
        return 0;
    }

    uint64_t avg_partition_size() const {
        return 0;
    }

    Bucketer bucketer() const {
        return m_bucketer;
    }

    std::vector<uint64_t> const& pilots() const {
        return m_pilots;
    }

    bits::bit_vector const& taken() const {
        return m_taken;
    }

    std::vector<uint64_t> const& free_slots() const {
        return m_free_slots;
    }

    void swap(internal_memory_builder_single_phf& other) {
        std::swap(m_seed, other.m_seed);
        std::swap(m_num_keys, other.m_num_keys);
        std::swap(m_num_buckets, other.m_num_buckets);
        std::swap(m_table_size, other.m_table_size);
        m_bucketer.swap(other.m_bucketer);
        m_pilots.swap(other.m_pilots);
        m_free_slots.swap(other.m_free_slots);
    }

    template <typename Visitor>
    void visit(Visitor& visitor) const {
        visit_impl(visitor, *this);
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visit_impl(visitor, *this);
    }

    static uint64_t estimate_num_bytes_for_construction(const uint64_t num_keys,
                                                        build_configuration const& config) {
        uint64_t table_size = static_cast<double>(num_keys) / config.alpha;
        const uint64_t num_buckets = (config.num_buckets == constants::invalid_num_buckets)
                                         ? compute_num_buckets(num_keys, config.lambda)
                                         : config.num_buckets;

        uint64_t num_bytes_for_map = num_keys * sizeof(bucket_payload_pair)          // pairs
                                     + (num_keys + num_buckets) * sizeof(uint64_t);  // buckets

        uint64_t num_bytes_for_search =
            num_buckets * sizeof(uint64_t)                                       // pilots
            + num_buckets * sizeof(uint64_t)                                     // buckets
            + (config.minimal ? (table_size - num_keys) * sizeof(uint64_t) : 0)  // free_slots
            + num_keys * sizeof(uint64_t)                                        // hashes
            + table_size / 8;                                                    // bitmap taken

        return std::max<uint64_t>(num_bytes_for_map, num_bytes_for_search);
    }

private:
    template <typename Visitor, typename T>
    static void visit_impl(Visitor& visitor, T&& t) {
        visitor.visit(t.m_seed);
        visitor.visit(t.m_num_keys);
        visitor.visit(t.m_num_buckets);
        visitor.visit(t.m_table_size);
        visitor.visit(t.m_bucketer);
        visitor.visit(t.m_pilots);
        visitor.visit(t.m_free_slots);
    }

    uint64_t m_seed;
    uint64_t m_num_keys;
    uint64_t m_num_buckets;
    uint64_t m_table_size;

    Bucketer m_bucketer;

    bits::bit_vector m_taken;
    std::vector<uint64_t> m_pilots;
    std::vector<uint64_t> m_free_slots;

    template <typename RandomAccessIterator>
    struct hash_generator {
        hash_generator(RandomAccessIterator keys, uint64_t seed) : m_iterator(keys), m_seed(seed) {}

        inline auto operator*() {
            return hasher_type::hash(*m_iterator, m_seed);
        }

        inline void operator++() {
            ++m_iterator;
        }

        inline hash_generator operator+(uint64_t offset) const {
            return hash_generator(m_iterator + offset, m_seed);
        }

    private:
        RandomAccessIterator m_iterator;
        uint64_t m_seed;
    };

    typedef std::vector<bucket_payload_pair> pairs_t;

    struct buckets_iterator_t {
        buckets_iterator_t(std::vector<std::vector<uint64_t>> const& buffers)
            : m_buffers_it(buffers.end() - 1), m_bucket_size(buffers.size()) {
            m_bucket.init(m_buffers_it->data(), m_bucket_size);
            skip_empty_buckets();
        }

        inline void operator++() {
            uint64_t const* begin = m_bucket.begin() + m_bucket_size;
            uint64_t const* end = m_buffers_it->data() + m_buffers_it->size();
            m_bucket.init(begin, m_bucket_size);
            if ((m_bucket.begin() - 1) == end and m_bucket_size != 0) {
                --m_bucket_size;
                --m_buffers_it;
                skip_empty_buckets();
            }
        }

        inline bucket_t operator*() const {
            return m_bucket;
        }

    private:
        std::vector<std::vector<uint64_t>>::const_iterator m_buffers_it;
        bucket_size_type m_bucket_size;
        bucket_t m_bucket;

        void skip_empty_buckets() {
            while (m_bucket_size != 0 and m_buffers_it->empty()) {
                --m_bucket_size;
                --m_buffers_it;
            }
            if (m_bucket_size != 0) m_bucket.init(m_buffers_it->data(), m_bucket_size);
        }
    };

    struct buckets_t {
        buckets_t() : m_buffers(MAX_BUCKET_SIZE), m_num_buckets(0) {}

        template <typename HashIterator>
        void add(bucket_id_type bucket_id, uint64_t bucket_size, HashIterator hashes) {
            assert(bucket_size > 0);
            uint64_t i = bucket_size - 1;
            assert(i < MAX_BUCKET_SIZE);
            m_buffers[i].push_back(bucket_id);
            for (uint64_t k = 0; k != bucket_size; ++k, ++hashes) m_buffers[i].push_back(*hashes);
            ++m_num_buckets;
        }

        uint64_t num_buckets() const {
            return m_num_buckets;
        };

        buckets_iterator_t begin() const {
            return buckets_iterator_t(m_buffers);
        }

        void print_bucket_size_distribution() {
            uint64_t max_bucket_size = (*(begin())).size();
            std::cout << " == max bucket size = " << max_bucket_size << std::endl;
            for (int64_t i = max_bucket_size - 1; i >= 0; --i) {
                uint64_t t = i + 1;
                uint64_t num_buckets_of_size_t = m_buffers[i].size() / (t + 1);
                std::cout << " == num_buckets of size " << t << " = " << num_buckets_of_size_t
                          << std::endl;
            }
        }

    private:
        std::vector<std::vector<uint64_t>> m_buffers;
        uint64_t m_num_buckets;
    };

    struct pilots_wrapper_t {
        pilots_wrapper_t(std::vector<uint64_t>& pilots) : m_pilots(pilots) {}

        inline void emplace_back(bucket_id_type bucket_id, uint64_t pilot) {
            m_pilots[bucket_id] = pilot;
        }

    private:
        std::vector<uint64_t>& m_pilots;
    };

    template <typename RandomAccessIterator>
    void map_sequential(RandomAccessIterator hashes, uint64_t num_keys,
                        std::vector<pairs_t>& pairs_blocks) const {
        pairs_t pairs(num_keys);
        RandomAccessIterator begin = hashes;
        for (uint64_t i = 0; i != num_keys; ++i, ++begin) {
            auto hash = *begin;
            auto bucket_id = m_bucketer.bucket(hash.first());
            pairs[i] = {static_cast<bucket_id_type>(bucket_id), hash.second()};
        }
        std::sort(pairs.begin(), pairs.end());
        pairs_blocks.resize(1);
        pairs_blocks.front().swap(pairs);
    }

    template <typename RandomAccessIterator>
    void map_parallel(RandomAccessIterator hashes, uint64_t num_keys,
                      std::vector<pairs_t>& pairs_blocks, build_configuration const& config) const {
        pairs_blocks.resize(config.num_threads);
        uint64_t num_keys_per_thread = num_keys / config.num_threads;

        auto exe = [&](uint64_t tid) {
            auto& local_pairs = pairs_blocks[tid];
            RandomAccessIterator begin = hashes + tid * num_keys_per_thread;
            uint64_t local_num_keys = (tid != config.num_threads - 1)
                                          ? num_keys_per_thread
                                          : (num_keys - tid * num_keys_per_thread);
            local_pairs.resize(local_num_keys);
            for (uint64_t local_i = 0; local_i != local_num_keys; ++local_i, ++begin) {
                auto hash = *begin;
                auto bucket_id = m_bucketer.bucket(hash.first());
                local_pairs[local_i] = {static_cast<bucket_id_type>(bucket_id), hash.second()};
            }
            std::sort(local_pairs.begin(), local_pairs.end());
        };

        std::vector<std::thread> threads(config.num_threads);
        for (uint64_t i = 0; i != config.num_threads; ++i) threads[i] = std::thread(exe, i);
        for (auto& t : threads) {
            if (t.joinable()) t.join();
        }
    }

    template <typename RandomAccessIterator>
    void map(RandomAccessIterator hashes, uint64_t num_keys, std::vector<pairs_t>& pairs_blocks,
             build_configuration const& config) const {
        if (config.num_threads > 1 and num_keys >= config.num_threads) {
            map_parallel(hashes, num_keys, pairs_blocks, config);
        } else {
            map_sequential(hashes, num_keys, pairs_blocks);
        }
    }
};

}  // namespace pthash