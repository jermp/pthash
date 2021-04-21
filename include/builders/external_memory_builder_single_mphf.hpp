#pragma once

#include "../builders/util.hpp"
#include "../utils/bucketers.hpp"
#include "../utils/logger.hpp"
#include "../utils/fill_free_slots.hpp"
#include "../utils/hasher.hpp"
#include "external_memory_util.hpp"

namespace pthash {

template <typename Hasher>
struct external_memory_builder_single_mphf {
    typedef Hasher hasher_type;
    static const uint64_t cache_size = 1000;

    template <typename Iterator>
    build_timings build_from_keys(Iterator keys, uint64_t num_keys,
                                  build_configuration const& config) {
        if (config.alpha == 0 or config.alpha > 1.0) {
            throw std::invalid_argument("load factor must be > 0 and <= 1.0");
        }

        build_timings time;
        uint64_t table_size = static_cast<double>(num_keys) / config.alpha;
        if ((table_size & (table_size - 1)) == 0) table_size += 1;
        uint64_t num_buckets = std::ceil((config.c * num_keys) / std::log2(num_keys));

        if (sizeof(bucket_id_type) != sizeof(uint64_t) and
            num_buckets > (1ULL << (sizeof(bucket_id_type) * 8))) {
            throw std::runtime_error(
                "using too many buckets: change bucket_id_type to uint64_t or use a smaller c");
        }

        m_num_keys = num_keys;
        m_table_size = table_size;
        m_num_buckets = num_buckets;
        m_seed = config.seed == constants::invalid_seed ? random_value() : config.seed;
        m_bucketer.init(num_buckets);

        uint64_t available_ram = sysconf(_SC_PAGESIZE) * sysconf(_SC_PHYS_PAGES);
        uint64_t ram = static_cast<double>(available_ram) * 0.75;
        uint64_t buckets_sizes_bytes =
            num_buckets * sizeof(typename decltype(m_buckets_sizes)::value_type);
        uint64_t pilots_bytes = num_buckets * sizeof(typename decltype(m_pilots)::value_type);
        uint64_t bitmap_taken_bytes = (table_size + 64 - 1) / 8;
        uint64_t hashed_pilots_cache_bytes = cache_size * sizeof(uint64_t);
        if (buckets_sizes_bytes + pilots_bytes + bitmap_taken_bytes + hashed_pilots_cache_bytes >=
            ram) {
            throw std::runtime_error("not enough RAM available");
        }

        if (config.verbose_output) {
            std::cout << "c = " << config.c << std::endl;
            std::cout << "alpha = " << config.alpha << std::endl;
            std::cout << "num_keys = " << num_keys << std::endl;
            std::cout << "table_size = " << table_size << std::endl;
            std::cout << "num_buckets = " << num_buckets << std::endl;
            std::cout << "using " << static_cast<double>(num_keys * sizeof(record)) / 1000000000
                      << " GB of disk space" << std::endl;
        }

        {
            auto start = clock_type::now();
            {
                auto start = clock_type::now();
                compute_bucket_sizes(keys, config.verbose_output);
                auto stop = clock_type::now();
                if (config.verbose_output) {
                    std::cout << " == computing bucket sizes took: " << seconds(stop - start)
                              << " seconds" << std::endl;
                    std::cout << " == max bucket size = " << m_max_bucket_size << std::endl;
                }
            }
            {
                auto start = clock_type::now();
                ram -= buckets_sizes_bytes;
                form_blocks(keys, ram, config.tmp_dir, config.verbose_output);
                auto stop = clock_type::now();
                if (config.verbose_output) {
                    std::cout << " == forming, sorting, and writing " << m_meta_blocks.size()
                              << " block(s) took: " << seconds(stop - start) << " seconds"
                              << std::endl;
                }
            }
            auto stop = clock_type::now();
            time.mapping_ordering_seconds += seconds(stop - start);
            if (config.verbose_output) {
                std::cout << " == map+sort took " << time.mapping_ordering_seconds << " seconds"
                          << std::endl;
            }
        }

        {
            auto start = clock_type::now();
            ram += buckets_sizes_bytes;
            ram -= hashed_pilots_cache_bytes;
            ram -= bitmap_taken_bytes;
            ram -= pilots_bytes;
            merge_blocks_and_search(ram, config.verbose_output);
            auto stop = clock_type::now();
            time.searching_seconds = seconds(stop - start);
            if (config.verbose_output) {
                std::cout << " == search took " << time.searching_seconds << " seconds"
                          << std::endl;
            }
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
    uint64_t m_num_buckets;
    uint64_t m_max_bucket_size;

    skew_bucketer m_bucketer;
    std::vector<uint8_t>  // Assume that max block size is < 256 (it should always be the case)
        m_buckets_sizes;
    std::vector<meta_block> m_meta_blocks;

    std::vector<uint64_t> m_pilots;
    std::vector<uint64_t> m_free_slots;

    template <typename Iterator>
    void compute_bucket_sizes(Iterator keys, bool verbose_output) {
        constexpr uint64_t step = 100 * 1000000;
        m_buckets_sizes.resize(m_num_buckets, 0);
        m_max_bucket_size = 0;
        for (uint64_t i = 0; i != m_num_keys; ++i, ++keys) {
            auto const& key = *keys;
            auto hash = hasher_type::hash(key, m_seed);
            auto bucket = m_bucketer.bucket(hash.first());
            uint64_t bucket_size = m_buckets_sizes[bucket] + 1;
            if (bucket_size > m_max_bucket_size) m_max_bucket_size = bucket_size;
            m_buckets_sizes[bucket] = bucket_size;
            if (verbose_output and i > 0 and i % step == 0) {
                std::cout << "processed " << i << " lines of input" << std::endl;
            }
        }
    }

    template <typename Iterator>
    void form_blocks(Iterator keys, uint64_t ram, std::string const& dir_name,
                     bool verbose_output) {
        constexpr uint64_t step = 100 * 1000000;
        uint64_t num_records_per_block = ram / sizeof(record) /
                                         (2 + 1  // + 1 because sorting is not in-place
                                         );
        std::vector<record> input, output;
        input.reserve(num_records_per_block);
        output.reserve(num_records_per_block);

        uint64_t num_threads_for_sorting = std::thread::hardware_concurrency() - 1;
        parallel_record_sorter sorter(m_max_bucket_size, num_threads_for_sorting);
        std::unique_ptr<std::thread> handle;

        auto sort_and_write_block = [&]() {
            // auto start = clock_type::now();
            sorter.sort(output);
            // auto stop = clock_type::now();
            // if (verbose_output) {
            //     std::cout << " == sorting block took " << seconds(stop - start) << std::endl;
            // }
            std::string filename(dir_name + "/pthash.temp." + std::to_string(m_meta_blocks.size()));
            m_meta_blocks.emplace_back(filename, output.size());
            // start = clock_type::now();
            std::ofstream out(filename.c_str(), std::ofstream::out | std::ofstream::binary);
            if (!out.is_open()) throw std::runtime_error("cannot open file");
            out.write(reinterpret_cast<char const*>(output.data()),
                      static_cast<std::streamsize>(output.size() * sizeof(record)));
            out.close();
            // stop = clock_type::now();
            // if (verbose_output) {
            //     std::cout << " == writing block took " << seconds(stop - start) << std::endl;
            // }
            output.clear();
        };

        auto wait_and_write = [&]() {
            wait(handle);
            output.swap(input);
            input.clear();
            handle = async(sort_and_write_block);
        };

        for (uint64_t i = 0; i != m_num_keys; ++i, ++keys) {
            auto const& key = *keys;
            auto hash = hasher_type::hash(key, m_seed);
            bucket_id_type bucket_id = m_bucketer.bucket(hash.first());
            uint8_t bucket_size = m_buckets_sizes[bucket_id];
            assert(bucket_size <= m_max_bucket_size);
            input.push_back({bucket_size, bucket_id, hash.second()});
            if (input.size() == num_records_per_block) wait_and_write();
            if (verbose_output and i > 0 and i % step == 0) {
                std::cout << "processed " << i << " lines of input" << std::endl;
            }
        }
        if (!input.empty()) wait_and_write();
        wait(handle);

        std::vector<uint8_t>().swap(m_buckets_sizes);
    }

    void merge_blocks_and_search(uint64_t ram, bool verbose_output) {
        uint64_t num_records_per_block = ram /
                                         (m_meta_blocks.size()  // input blocks
                                          + 1                   // merged block
                                          + 1                   // input block
                                          ) /
                                         sizeof(record);

        heap<cursor, cursor_comparator> cursors;
        for (uint64_t id = 0; id != m_meta_blocks.size(); ++id) {
            auto& meta_block = m_meta_blocks[id];
            meta_block.open();
            meta_block.load(num_records_per_block);
            cursor c{meta_block.buffer().begin(), meta_block.buffer().end(), id};
            cursors.push(c);
        }

        std::vector<record> input, merged;
        input.reserve(num_records_per_block + m_max_bucket_size);
        merged.reserve(num_records_per_block);

        bit_vector_builder taken(m_table_size);  // occupied slots
        std::vector<uint64_t> local;
        local.reserve(m_max_bucket_size);
        m_pilots.resize(m_num_buckets, 0);
        __uint128_t M = fastmod::computeM_u64(m_table_size);

        logger log(m_num_keys, m_table_size, m_num_buckets);
        uint64_t processed_buckets = 0;
        std::unique_ptr<std::thread> handle;

        std::vector<uint64_t> hashed_pilots_cache(cache_size);
        for (uint64_t pilot = 0; pilot != cache_size; ++pilot) {
            hashed_pilots_cache[pilot] = default_hash64(pilot, m_seed);
        }

        auto search = [&]() {
            // NOTE: maintain the invariant that at the beginning of the search
            // we always start with a new bucket.
            uint64_t base = 0;
            while (base != input.size()) {
                assert(base <= input.size());
                bucket_id_type bucket_id = input[base].bucket_id;
                uint64_t bucket_size = input[base].bucket_size;
                assert(bucket_size > 0);
                if (bucket_size > input.size() - base) break;

                // check for collisions
                assert(base + bucket_size <= input.size());
                auto begin = input.begin() + base;
                auto end = begin + bucket_size;
                auto it = std::adjacent_find(
                    begin, end, [](record const& x, record const& y) { return x.hash == y.hash; });
                if (it != end) throw seed_runtime_error();

                uint64_t pilot = 0;
                for (; true; ++pilot) {
                    uint64_t hashed_pilot = PTH_LIKELY(pilot < cache_size)
                                                ? hashed_pilots_cache[pilot]
                                                : default_hash64(pilot, m_seed);

                    uint64_t j = 0;
                    for (; j != bucket_size; ++j) {
                        auto hash = input[base + j].hash;
                        auto slot = fastmod::fastmod_u64(hash ^ hashed_pilot, M, m_table_size);
                        if (taken.get(slot)) {
                            local.clear();
                            break;
                        }
                        local.push_back(slot);
                    }

                    if (j == bucket_size) {  // all keys do not have collisions with taken
                        // check for in-bucket collisions
                        std::sort(local.begin(), local.end());
                        auto it = std::adjacent_find(local.begin(), local.end());
                        if (it != local.end()) {  // in-bucket collision detected
                            local.clear();
                            continue;  // try the next pilot
                        }
                        m_pilots[bucket_id] = pilot;
                        for (auto slot : local) {
                            assert(taken.get(slot) == false);
                            taken.set(slot, true);
                        }
                        local.clear();
                        break;
                    }
                }

                if (verbose_output) log.update(processed_buckets, bucket_size, pilot);
                ++processed_buckets;
                base += bucket_size;
            }

            // copy last records from previous block because they belong
            // to the next bucket
            uint64_t tail_size = input.size() - base;
            std::vector<record> tmp;
            tmp.reserve(tail_size);
            tmp.assign(input.begin() + base, input.end());
            assert(tmp.size() == tail_size);
            input.clear();
            std::copy(tmp.begin(), tmp.end(), std::back_inserter(input));
        };

        auto wait_and_search = [&]() {
            wait(handle);
            input.insert(input.end(), merged.begin(), merged.end());
            assert(std::is_sorted(input.begin(), input.end(), record_comparator()));
            merged.clear();
            handle = async(search);
        };

        while (!cursors.empty()) {
            auto& top = cursors.top();
            auto max = *(top.begin);
            merged.push_back(max);
            if (merged.size() == num_records_per_block) wait_and_search();
            ++(top.begin);
            if (top.begin == top.end) {
                auto& meta_block = m_meta_blocks[top.id];
                if (meta_block.eof()) {
                    meta_block.release();
                    meta_block.close_and_remove();
                    cursors.pop();
                } else {
                    meta_block.load(num_records_per_block);
                    top.begin = meta_block.buffer().begin();
                    top.end = meta_block.buffer().end();
                }
            }
            cursors.heapify();
        }
        if (!merged.empty()) wait_and_search();
        wait(handle);

        if (verbose_output) log.finalize(processed_buckets);

        fill_free_slots(taken, m_num_keys, m_free_slots);
    }
};

}  // namespace pthash