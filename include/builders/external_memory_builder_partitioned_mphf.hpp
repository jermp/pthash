#pragma once

#include <thread>

#include "../../external/mm_file/include/mm_file/mm_file.hpp"
#include "internal_memory_builder_single_mphf.hpp"
#include "internal_memory_builder_partitioned_mphf.hpp"
#include "util.hpp"

namespace pthash {

template <typename Hasher>
struct external_memory_builder_partitioned_mphf {
    typedef Hasher hasher_type;
    typedef typename hasher_type::hash_type hash_type;

    template <typename Iterator>
    build_timings build_from_keys(Iterator keys, uint64_t num_keys,
                                  build_configuration const& config) {
        if (config.num_partitions == 0) {
            throw std::invalid_argument("number of partitions must be > 0");
        }

        auto start = clock_type::now();

        build_timings timings;
        uint64_t num_partitions = config.num_partitions;
        if (config.verbose_output) std::cout << "num_partitions " << num_partitions << std::endl;

        m_seed = config.seed == constants::invalid_seed ? random_value() : config.seed;
        m_num_keys = num_keys;
        m_num_partitions = num_partitions;
        m_bucketer.init(num_partitions);
        m_offsets.resize(num_partitions);
        m_builders.resize(num_partitions);

        std::vector<meta_partition> partitions;
        partitions.reserve(num_partitions);
        double average_partition_size = static_cast<double>(num_keys) / num_partitions;
        for (uint64_t id = 0; id != num_partitions; ++id) {
            partitions.emplace_back(config.tmp_dir, id);
            partitions.back().reserve(1.5 * average_partition_size);
        }

        size_t available_ram = sysconf(_SC_PAGESIZE) * sysconf(_SC_PHYS_PAGES);
        size_t ram = static_cast<double>(available_ram) * 0.75;
        size_t bytes = num_partitions * sizeof(meta_partition);
        if (bytes >= ram) throw std::runtime_error("Not enough RAM available");

        for (uint64_t i = 0; i != num_keys; ++i, ++keys) {
            auto const& key = *keys;
            auto hash = hasher_type::hash(key, m_seed);
            auto b = m_bucketer.bucket(hash.mix());
            partitions[b].push_back(hash);
            bytes += sizeof(hash_type);
            if (bytes >= ram) {
                for (auto& partition : partitions) partition.flush();
                bytes = num_partitions * sizeof(meta_partition);
            }
        }

        for (auto& partition : partitions) partition.release();

        timings.partitioning_seconds += seconds(clock_type::now() - start);

        for (uint64_t i = 0, cumulative_size = 0; i != num_partitions; ++i) {
            auto const& partition = partitions[i];
            m_offsets[i] = cumulative_size;
            cumulative_size += partition.size();
        }

        auto partition_config = config;
        partition_config.seed = m_seed;
        uint64_t num_buckets_single_mphf = std::ceil((config.c * num_keys) / std::log2(num_keys));
        partition_config.num_buckets =
            static_cast<double>(num_buckets_single_mphf) / num_partitions;

        if (config.num_threads > 1) {  // parallel
            partition_config.verbose_output = false;
            bytes = num_partitions * sizeof(meta_partition);
            std::vector<std::vector<hash_type>> in_memory_partitions;
            uint64_t i = 0;

            auto build_partitions = [&]() {
                if (config.verbose_output) {
                    std::cout << "processing " << in_memory_partitions.size() << "/"
                              << num_partitions << " partitions..." << std::endl;
                }
                partition_config.num_partitions = in_memory_partitions.size();
                auto t = internal_memory_builder_partitioned_mphf<hasher_type>::build_partitions(
                    in_memory_partitions.begin(),
                    m_builders.begin() + i - in_memory_partitions.size(), partition_config);
                timings.mapping_ordering_seconds += t.mapping_ordering_seconds;
                timings.searching_seconds += t.searching_seconds;
                in_memory_partitions.clear();
                bytes = num_partitions * sizeof(meta_partition);
            };

            for (; i != num_partitions; ++i) {
                uint64_t size = partitions[i].size();
                uint64_t partition_bytes = over_estimate_num_bytes(size, partition_config);
                if (bytes + partition_bytes >= ram) build_partitions();
                std::vector<hash_type> p(size);
                std::ifstream in(partitions[i].filename().c_str(), std::ifstream::binary);
                if (!in.is_open()) throw std::runtime_error("cannot open file");
                in.read(reinterpret_cast<char*>(p.data()),
                        static_cast<std::streamsize>(size * sizeof(hash_type)));
                in.close();
                std::remove(partitions[i].filename().c_str());
                in_memory_partitions.push_back(std::move(p));
                bytes += partition_bytes;
            }
            if (!in_memory_partitions.empty()) build_partitions();
            std::vector<std::vector<hash_type>>().swap(in_memory_partitions);

        } else {  // sequential
            for (uint64_t i = 0; i != num_partitions; ++i) {
                if (config.verbose_output) {
                    std::cout << "processing partition " << i << "..." << std::endl;
                }
                mm::file_source<hash_type> partition(partitions[i].filename(),
                                                     mm::advice::sequential);
                auto t = m_builders[i].build_from_hashes(partition.data(), partition.size(),
                                                         partition_config);
                partition.close();
                std::remove(partitions[i].filename().c_str());
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

    uint64_t num_partitions() const {
        return m_num_partitions;
    }

    uniform_bucketer bucketer() const {
        return m_bucketer;
    }

    std::vector<uint64_t> const& offsets() const {
        return m_offsets;
    }

    std::vector<internal_memory_builder_single_mphf<hasher_type>> const& builders() const {
        return m_builders;
    }

private:
    uint64_t m_seed;
    uint64_t m_num_keys;
    uint64_t m_num_partitions;
    uniform_bucketer m_bucketer;

    std::vector<uint64_t> m_offsets;
    std::vector<internal_memory_builder_single_mphf<hasher_type>> m_builders;

    size_t over_estimate_num_bytes(uint64_t num_keys, build_configuration const& config) const {
        uint64_t table_size = static_cast<double>(num_keys) / config.alpha;
        if ((table_size & (table_size - 1)) == 0) table_size += 1;
        uint64_t num_buckets = (config.num_buckets == constants::invalid_num_buckets)
                                   ? (std::ceil((config.c * num_keys) / std::log2(num_keys)))
                                   : config.num_buckets;
        typedef typename internal_memory_builder_single_mphf<hasher_type>::bucket bucket_type;
        size_t bytes = num_buckets * sizeof(uint64_t)                    // pilots
                       + num_buckets * sizeof(bucket_type)               // buckets
                       + 2 * (table_size - num_keys) * sizeof(uint64_t)  // free_slots
                       + 4 * (num_keys + 1) * sizeof(uint64_t)           // hashes
                       + (table_size + 64 - 1) / 8;                      // bitmap taken
        return bytes;
    }

    struct meta_partition {
        meta_partition(std::string const& dir_name, uint64_t id)
            : m_filename(dir_name + "/pthash.temp." + std::to_string(id)), m_size(0) {}

        void push_back(hash_type hash) {
            m_hashes.push_back(hash);
        }

        std::string const& filename() const {
            return m_filename;
        }

        void flush() {
            if (m_hashes.empty()) return;
            m_size += m_hashes.size();
            std::ofstream out(m_filename.c_str(), std::ofstream::binary | std::ofstream::app);
            if (!out.is_open()) throw std::runtime_error("cannot open file");
            out.write(reinterpret_cast<char const*>(m_hashes.data()),
                      m_hashes.size() * sizeof(hash_type));
            out.close();
            m_hashes.clear();
        }

        void reserve(uint64_t n) {
            m_hashes.reserve(n);
        }

        void release() {
            flush();
            std::vector<hash_type>().swap(m_hashes);
        }

        uint64_t size() const {
            return m_size;
        }

    private:
        std::string m_filename;
        std::vector<hash_type> m_hashes;
        uint64_t m_size;
    };
};

}  // namespace pthash