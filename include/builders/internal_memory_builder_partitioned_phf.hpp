#pragma once

#include "util.hpp"
#include "internal_memory_builder_single_phf.hpp"

namespace pthash {

template <typename Hasher, typename Bucketer>
struct internal_memory_builder_partitioned_phf {
    typedef Hasher hasher_type;
    typedef Bucketer bucketer_type;

    template <typename Iterator>
    build_timings build_from_keys(Iterator keys, const uint64_t num_keys,
                                  build_configuration const& config) {
        assert(num_keys > 1);
        util::check_hash_collision_probability<Hasher>(num_keys);

        const uint64_t num_partitions = config.num_partitions;
        if (config.verbose_output) std::cout << "num_partitions " << num_partitions << std::endl;
        if (num_partitions == 0) throw std::invalid_argument("number of partitions must be > 0");

        if (config.alpha != 1.0 and config.dense_partitioning) {
            throw std::runtime_error("alpha must be 1.0 for dense partitioning");
        }

        auto start = clock_type::now();

        build_timings timings;

        m_seed = config.seed == constants::invalid_seed ? random_value() : config.seed;
        m_num_keys = num_keys;
        m_table_size = 0;
        m_num_partitions = num_partitions;
        m_bucketer.init(num_partitions);
        m_offsets.resize(num_partitions + 1);
        m_builders.resize(num_partitions);

        double average_partition_size = static_cast<double>(num_keys) / num_partitions;
        if (average_partition_size < constants::min_partition_size and num_partitions > 1) {
            throw std::runtime_error("average partition size is too small: use less partitions");
        }
        std::vector<std::vector<typename hasher_type::hash_type>> partitions(num_partitions);
        for (auto& partition : partitions) partition.reserve(1.5 * average_partition_size);

        progress_logger logger(num_keys, " == partitioned ", " keys", config.verbose_output);
        for (uint64_t i = 0; i != num_keys; ++i, ++keys) {
            auto const& key = *keys;
            auto hash = hasher_type::hash(key, m_seed);
            auto b = m_bucketer.bucket(hash.mix());
            partitions[b].push_back(hash);
            logger.log();
        }
        logger.finalize();

        uint64_t cumulative_size = 0;
        for (uint64_t i = 0; i != num_partitions; ++i) {
            auto const& partition = partitions[i];
            if (partition.size() <= 1) {
                throw std::runtime_error(
                    "each partition must contain more than one key: use less partitions");
            }
            uint64_t table_size = static_cast<double>(partition.size()) / config.alpha;
            if ((table_size & (table_size - 1)) == 0) {
                std::cerr << "Warning: table_size = " << table_size << ", a power of 2..."
                          << std::endl;
                table_size += 1;
            }
            m_table_size += table_size;
            m_offsets[i] = cumulative_size;
            cumulative_size += config.minimal_output ? partition.size() : table_size;
        }
        m_offsets[num_partitions] = cumulative_size;

        auto partition_config = config;
        partition_config.seed = m_seed;

        const uint64_t num_buckets_single_phf = compute_num_buckets(num_keys, config.lambda);
        const uint64_t num_buckets_per_partition =
            std::ceil(static_cast<double>(num_buckets_single_phf) / num_partitions);
        m_num_buckets_per_partition = num_buckets_per_partition;
        partition_config.num_buckets = num_buckets_per_partition;
        if (config.verbose_output) {
            std::cout << "num_buckets_per_partition = " << partition_config.num_buckets
                      << std::endl;
        }
        partition_config.verbose_output = false;
        partition_config.num_threads = 1;

        timings.partitioning_seconds = seconds(clock_type::now() - start);

        auto t = build_partitions(partitions.begin(), m_builders.begin(), partition_config,
                                  config.num_threads);
        timings.mapping_ordering_seconds = t.mapping_ordering_seconds;
        timings.searching_seconds = t.searching_seconds;

        return timings;
    }

    template <typename PartitionsIterator, typename BuildersIterator>
    static build_timings build_partitions(PartitionsIterator partitions, BuildersIterator builders,
                                          build_configuration const& config,
                                          const uint64_t num_threads) {
        build_timings timings;
        const uint64_t num_partitions = config.num_partitions;
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
                    thread_timings[i].mapping_ordering_seconds += t.mapping_ordering_seconds;
                    thread_timings[i].searching_seconds += t.searching_seconds;
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
                if (t.mapping_ordering_seconds > timings.mapping_ordering_seconds)
                    timings.mapping_ordering_seconds = t.mapping_ordering_seconds;
                if (t.searching_seconds > timings.searching_seconds)
                    timings.searching_seconds = t.searching_seconds;
            }
        } else {  // sequential
            for (uint64_t i = 0; i != num_partitions; ++i) {
                auto const& partition = partitions[i];
                builders[i].set_seed(config.seed);
                auto t = builders[i].build_from_hashes(partition.begin(), partition.size(), config);
                timings.mapping_ordering_seconds += t.mapping_ordering_seconds;
                timings.searching_seconds += t.searching_seconds;
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

    uniform_bucketer bucketer() const {
        return m_bucketer;
    }

    std::vector<uint64_t> const& offsets() const {
        return m_offsets;
    }

    std::vector<internal_memory_builder_single_phf<hasher_type, bucketer_type>> const& builders()
        const {
        return m_builders;
    }

    struct interleaving_pilots_iterator {
        interleaving_pilots_iterator(
            std::vector<internal_memory_builder_single_phf<hasher_type, bucketer_type>> const&
                builders,
            uint64_t m_curr_partition = 0, uint64_t curr_bucket_in_partition = 0)
            : m_builders(builders)
            , m_curr_partition(m_curr_partition)
            , m_curr_bucket_in_partition(curr_bucket_in_partition)
            , m_num_partitions(builders.size()) {}

        uint64_t operator*() const {
            auto const& pilots_of_partition = m_builders[m_curr_partition].pilots();
            return pilots_of_partition[m_curr_bucket_in_partition];
        }

        void operator++() {
            m_curr_partition += 1;
            if (m_curr_partition == m_num_partitions) {
                m_curr_partition = 0;
                m_curr_bucket_in_partition += 1;
            }
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
        std::vector<internal_memory_builder_single_phf<hasher_type, bucketer_type>> const&
            m_builders;
        uint64_t m_curr_partition;
        uint64_t m_curr_bucket_in_partition;
        uint64_t m_num_partitions;
    };

    interleaving_pilots_iterator interleaving_pilots_iterator_begin() const {
        return interleaving_pilots_iterator(m_builders);
    }

private:
    uint64_t m_seed;
    uint64_t m_num_keys;
    uint64_t m_table_size;
    uint64_t m_num_partitions;
    uint64_t m_num_buckets_per_partition;
    uniform_bucketer m_bucketer;
    std::vector<uint64_t> m_offsets;
    std::vector<internal_memory_builder_single_phf<hasher_type, bucketer_type>> m_builders;
};

}  // namespace pthash