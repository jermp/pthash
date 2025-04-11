#pragma once

#include "builders/util.hpp"
#include "builders/internal_memory_builder_single_phf.hpp"

namespace pthash {

template <typename Hasher, typename Bucketer>
struct internal_memory_builder_partitioned_phf {
    typedef Hasher hasher_type;
    typedef Bucketer bucketer_type;

    template <typename Iterator>
    build_timings build_from_keys(Iterator keys, const uint64_t num_keys,
                                  build_configuration const& config)  //
    {
        assert(num_keys > 0);
        util::check_hash_collision_probability<Hasher>(num_keys);

        const uint64_t avg_partition_size = compute_avg_partition_size(num_keys, config);
        const uint64_t num_partitions = compute_num_partitions(num_keys, avg_partition_size);
        assert(num_partitions > 0);

        auto start = clock_type::now();

        if (config.verbose) {
            std::cout << "num_partitions = " << num_partitions << std::endl;
            std::cout << "avg_partition_size = " << avg_partition_size << std::endl;
        }

        build_timings timings;

        m_seed = config.seed == constants::invalid_seed ? random_value() : config.seed;
        m_num_keys = num_keys;
        m_table_size = 0;
        m_num_partitions = num_partitions;
        m_bucketer.init(num_partitions);
        m_offsets.resize(num_partitions + 1);
        m_builders.resize(num_partitions);
        m_num_buckets_per_partition = compute_num_buckets(avg_partition_size, config.lambda);

        std::vector<std::vector<typename hasher_type::hash_type>> partitions(num_partitions);
        {
            /*
            This bound is by Raab and Steger: "Balls into Bins" — A Simple and Tight Analysis,
            (Thm. 1, with alpha = 1).
            */
            const uint64_t largest_partition_size_estimate =
                avg_partition_size + sqrt(2 * avg_partition_size * log(num_partitions));
            if (config.verbose) {
                std::cout << "largest_partition_size_estimate " << largest_partition_size_estimate
                          << std::endl;
            }
            for (auto& partition : partitions) partition.reserve(largest_partition_size_estimate);
        }

        if constexpr (std::is_same_v<typename Iterator::iterator_category,
                                     std::random_access_iterator_tag>)  //
        {
            parallel_hash_and_partition(keys, partitions, num_keys, config.num_threads, m_seed,
                                        num_partitions, m_bucketer);
        } else {
            for (uint64_t i = 0; i != num_keys; ++i, ++keys) {
                auto const& key = *keys;
                auto hash = hasher_type::hash(key, m_seed);
                auto b = m_bucketer.bucket(hash.mix());
                partitions[b].push_back(hash);
            }
        }

        uint64_t largest_partition_size = 0;
        for (auto const& partition : partitions) {
            if (partition.size() > largest_partition_size) {
                largest_partition_size = partition.size();
            }
        }

        uint64_t table_size_per_partition = constants::invalid_table_size;
        if (config.dense_partitioning) {
            // allocate avg_partition_size / alpha slots per partition
            table_size_per_partition =
                std::ceil(static_cast<double>(avg_partition_size) / config.alpha);
            if (largest_partition_size > table_size_per_partition) {
                table_size_per_partition = largest_partition_size;
            }
            m_table_size_per_partition = table_size_per_partition;
            m_table_size = table_size_per_partition * num_partitions;
        } else {
            uint64_t cumulative_size = 0;
            for (uint64_t i = 0; i != num_partitions; ++i) {
                auto const& partition = partitions[i];
                uint64_t table_size = static_cast<double>(partition.size()) / config.alpha;
                m_table_size += table_size;
                m_offsets[i] = cumulative_size;
                if (config.dense_partitioning) {
                    cumulative_size += table_size;
                } else {
                    cumulative_size += config.minimal ? partition.size() : table_size;
                }
            }
            m_offsets[num_partitions] = cumulative_size;
        }

        auto partition_config = config;
        partition_config.seed = m_seed;
        partition_config.num_buckets = m_num_buckets_per_partition;
        if (config.dense_partitioning) partition_config.table_size = table_size_per_partition;
        if (config.verbose) {
            if (config.dense_partitioning) {
                std::cout << "table_size_per_partition = " << table_size_per_partition << std::endl;
            }
            std::cout << "(largest_partition_size = " << largest_partition_size << ")" << std::endl;
            std::cout << "num_buckets_per_partition = " << partition_config.num_buckets
                      << std::endl;
        }
        partition_config.verbose = false;
        partition_config.num_threads = 1;

        timings.partitioning_microseconds = to_microseconds(clock_type::now() - start);

        auto t = build_partitions(partitions.begin(), m_builders.begin(), partition_config,
                                  config.num_threads, num_partitions);
        timings.mapping_ordering_microseconds = t.mapping_ordering_microseconds;
        timings.searching_microseconds = t.searching_microseconds;

        if (config.minimal) {
            auto start = clock_type::now();
            m_free_slots.clear();
            taken t(m_builders);
            assert(t.size() >= num_keys);
            m_free_slots.reserve(t.size() - num_keys);
            fill_free_slots(t, num_keys, m_free_slots, m_table_size);
            auto stop = clock_type::now();
            timings.searching_microseconds += to_microseconds(stop - start);
        }

        return timings;
    }

    template <typename RandomAccessIterator>
    static void parallel_hash_and_partition(
        RandomAccessIterator keys,
        std::vector<std::vector<typename hasher_type::hash_type>>& partitions,
        const uint64_t num_keys, const uint64_t num_threads, const uint64_t m_seed,
        const uint64_t num_partitions, const range_bucketer partitioner)  //
    {
        std::vector<std::vector<std::vector<typename hasher_type::hash_type>>> split;
        split.resize(num_threads);
        uint64_t partitions_per_thread = (num_partitions + num_threads - 1) / num_threads;
        uint64_t expected_cell_size = num_keys / (num_threads * num_threads);
        uint64_t cell_reserve = expected_cell_size + expected_cell_size / 20;
        for (auto& v : split) {
            v.resize(num_threads);
            for (auto& c : v) c.reserve(cell_reserve);
        }

        auto hash_and_split = [&](uint64_t id, uint64_t begin, uint64_t end) {
            for (; begin != end; ++begin) {
                typename hasher_type::hash_type hash = hasher_type::hash(keys[begin], m_seed);
                uint64_t partition = partitioner.bucket(hash.mix());
                uint64_t coloumn = partition / partitions_per_thread;
                split[id][coloumn].push_back(hash);
            }
        };

        auto merge_and_collect = [&](uint64_t id) {
            for (uint64_t row = 0; row < num_threads; ++row) {
                for (typename hasher_type::hash_type hash : split[row][id]) {
                    uint64_t partition = partitioner.bucket(hash.mix());
                    partitions[partition].push_back(hash);
                }
            }
        };

        std::vector<std::thread> threads(num_threads);
        const uint64_t num_keys_per_thread = (num_keys + num_threads - 1) / num_threads;
        for (uint64_t i = 0, begin = 0; i != num_threads; ++i) {
            uint64_t end = begin + num_keys_per_thread;
            if (end > num_keys) end = num_keys;
            threads[i] = std::thread(hash_and_split, i, begin, end);
            begin = end;
        }
        for (auto& t : threads) {
            if (t.joinable()) t.join();
        }

        for (uint64_t i = 0; i != num_threads; ++i) {
            threads[i] = std::thread(merge_and_collect, i);
        }
        for (auto& t : threads) {
            if (t.joinable()) t.join();
        }
    }

    template <typename PartitionsIterator, typename BuildersIterator>
    static build_timings build_partitions(PartitionsIterator partitions, BuildersIterator builders,
                                          build_configuration const& config,
                                          const uint64_t num_threads,
                                          const uint64_t num_partitions) {
        build_timings timings;
        assert(config.num_threads == 1);

        if (num_threads > 1) {  // parallel
            std::vector<std::thread> threads(num_threads);
            std::vector<build_timings> thread_timings(num_threads);

            auto exe = [&](uint64_t i, uint64_t begin, uint64_t end) {
                for (; begin != end; ++begin) {
                    auto const& partition = partitions[begin];
                    builders[begin].set_seed(config.seed);
                    auto t = builders[begin].build_from_hashes(partition.begin(), partition.size(),
                                                               config);
                    thread_timings[i].mapping_ordering_microseconds +=
                        t.mapping_ordering_microseconds;
                    thread_timings[i].searching_microseconds += t.searching_microseconds;
                }
            };

            const uint64_t num_partitions_per_thread =
                (num_partitions + num_threads - 1) / num_threads;
            for (uint64_t i = 0, begin = 0; i != num_threads; ++i) {
                uint64_t end = begin + num_partitions_per_thread;
                if (end > num_partitions) end = num_partitions;
                threads[i] = std::thread(exe, i, begin, end);
                begin = end;
            }

            for (auto& t : threads) {
                if (t.joinable()) t.join();
            }

            for (auto const& t : thread_timings) {
                if (t.mapping_ordering_microseconds > timings.mapping_ordering_microseconds)
                    timings.mapping_ordering_microseconds = t.mapping_ordering_microseconds;
                if (t.searching_microseconds > timings.searching_microseconds)
                    timings.searching_microseconds = t.searching_microseconds;
            }
        } else {  // sequential
            for (uint64_t i = 0; i != num_partitions; ++i) {
                auto const& partition = partitions[i];
                builders[i].set_seed(config.seed);
                auto t = builders[i].build_from_hashes(partition.begin(), partition.size(), config);
                timings.mapping_ordering_microseconds += t.mapping_ordering_microseconds;
                timings.searching_microseconds += t.searching_microseconds;
            }
        }

        return timings;
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
        return m_num_partitions;
    }

    uint64_t num_buckets_per_partition() const {
        return m_num_buckets_per_partition;
    }

    uint64_t table_size_per_partition() const {
        return m_table_size_per_partition;
    }

    range_bucketer bucketer() const {
        return m_bucketer;
    }

    std::vector<uint64_t> const& offsets() const {
        return m_offsets;
    }

    std::vector<uint64_t> const& free_slots() const {
        return m_free_slots;
    }

    std::vector<internal_memory_builder_single_phf<hasher_type, bucketer_type>> const& builders()
        const {
        return m_builders;
    }

    struct interleaving_pilots_iterator  //
    {
        /* Must define all the five properties, otherwise compilation fails. */
        using value_type = uint64_t;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = value_type&;
        using iterator_category = std::random_access_iterator_tag;

        interleaving_pilots_iterator(
            std::vector<internal_memory_builder_single_phf<hasher_type, bucketer_type>> const*
                builders,
            uint64_t m_curr_partition = 0, uint64_t curr_bucket_in_partition = 0)
            : m_builders(builders)
            , m_curr_partition(m_curr_partition)
            , m_curr_bucket_in_partition(curr_bucket_in_partition)
            , m_num_partitions(builders->size()) {}

        uint64_t operator*() const {
            auto const& pilots_of_partition = (*m_builders)[m_curr_partition].pilots();
            return pilots_of_partition[m_curr_bucket_in_partition];
        }

        interleaving_pilots_iterator& operator++() {
            m_curr_partition += 1;
            if (m_curr_partition == m_num_partitions) {
                m_curr_partition = 0;
                m_curr_bucket_in_partition += 1;
            }
            return *this;
        }

        bool operator==(interleaving_pilots_iterator const& rhs) const {
            return m_curr_partition == rhs.m_curr_partition and
                   m_curr_bucket_in_partition == rhs.m_curr_bucket_in_partition and
                   m_num_partitions == rhs.m_num_partitions;
        }

        bool operator!=(interleaving_pilots_iterator const& rhs) const {
            return !(*this == rhs);
        }

        interleaving_pilots_iterator operator+(const uint64_t n) const {
            uint64_t bucket = n / m_num_partitions + m_curr_bucket_in_partition;
            uint64_t partition = n % m_num_partitions + m_curr_partition;
            return interleaving_pilots_iterator(m_builders, partition, bucket);
        }

    private:
        std::vector<internal_memory_builder_single_phf<hasher_type, bucketer_type>> const*
            m_builders;
        uint64_t m_curr_partition;
        uint64_t m_curr_bucket_in_partition;
        uint64_t m_num_partitions;
    };

    /*
        Logically aggregate all "taken" bitmaps from all partitions.
    */
    struct taken {
        taken(std::vector<internal_memory_builder_single_phf<hasher_type, bucketer_type>> const&
                  builders)
            : m_builders(builders), m_size(0) {
            for (auto const& b : m_builders) m_size += b.taken().num_bits();
        }

        struct iterator {
            iterator(taken const* taken, const uint64_t pos = 0)
                : m_taken(taken)
                , m_curr_pos(pos)
                , m_curr_offset(0)
                , m_curr_partition(0)  //
            {
                while (m_curr_pos >=
                       m_curr_offset + m_taken->m_builders[m_curr_partition].taken().num_bits()) {
                    m_curr_offset += m_taken->m_builders[m_curr_partition].taken().num_bits();
                    m_curr_partition += 1;
                }
            }

            bool operator*() {
                assert(m_curr_pos < m_taken->size());
                assert(m_curr_pos >= m_curr_offset);
                uint64_t offset = m_curr_pos - m_curr_offset;
                if (offset == m_taken->m_builders[m_curr_partition].taken().num_bits()) {
                    m_curr_offset += m_taken->m_builders[m_curr_partition].taken().num_bits();
                    m_curr_partition += 1;
                    offset = 0;
                }
                assert(m_curr_partition < m_taken->m_builders.size());
                auto const& t = m_taken->m_builders[m_curr_partition].taken();
                assert(offset < t.num_bits());
                return t.get(offset);
            }

            void operator++() {
                m_curr_pos += 1;
            }

        private:
            taken const* m_taken;
            uint64_t m_curr_pos;
            uint64_t m_curr_offset;
            uint64_t m_curr_partition;
        };

        iterator get_iterator_at(const uint64_t pos) const {
            return iterator(this, pos);
        }

        uint64_t size() const {
            return m_size;
        }

    private:
        std::vector<internal_memory_builder_single_phf<hasher_type, bucketer_type>> const&
            m_builders;
        uint64_t m_size;
    };

    interleaving_pilots_iterator interleaving_pilots_iterator_begin() const {
        return interleaving_pilots_iterator(&m_builders);
    }

private:
    uint64_t m_seed;
    uint64_t m_num_keys;
    uint64_t m_table_size;
    uint64_t m_num_partitions;
    uint64_t m_num_buckets_per_partition;
    uint64_t m_table_size_per_partition;
    range_bucketer m_bucketer;
    std::vector<uint64_t> m_offsets;
    std::vector<uint64_t> m_free_slots;  // for dense partitioning
    std::vector<internal_memory_builder_single_phf<hasher_type, bucketer_type>> m_builders;
};

}  // namespace pthash