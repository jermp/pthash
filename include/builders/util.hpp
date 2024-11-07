#pragma once

#include <fstream>
#include <thread>
#include <cmath>  // for exp, log, lgamma

#include "include/utils/logger.hpp"

namespace pthash {

#ifdef DPTHASH_ENABLE_LARGE_BUCKET_ID_TYPE
typedef uint64_t bucket_id_type;
#else
typedef uint32_t bucket_id_type;
#endif
typedef uint8_t bucket_size_type;

constexpr bucket_size_type MAX_BUCKET_SIZE = 255;

enum pthash_search_type { xor_displacement, add_displacement };

static inline std::string get_tmp_builder_filename(std::string const& dir_name, uint64_t id) {
    return dir_name + "/pthash.temp." + std::to_string(id) + ".builder";
}

/*
    Evaluate Poisson probability mass function (pmf) in the log_e domain.
    P(k,lambda) = e^-lambda * lambda^k / k! = e^-lambda * lambda^k / gamma(k+1) =
                = e^(log_e(e^-lambda * lambda^k / gamma(k+1))) =
                = e^(k * log_e(lambda) - log_e(gamma(k+1)) - lambda)
*/
static inline double poisson_pmf(double k, double lambda) {
    return exp(k * log(lambda) - lgamma(k + 1.0) - lambda);
}

struct build_timings {
    build_timings()
        : partitioning_microseconds(0.0)
        , mapping_ordering_microseconds(0.0)
        , searching_microseconds(0.0)
        , encoding_microseconds(0.0) {}

    double partitioning_microseconds;
    double mapping_ordering_microseconds;
    double searching_microseconds;
    double encoding_microseconds;
};

struct build_configuration {
    build_configuration()
        : lambda(4.5)
        , alpha(0.98)
        , search(pthash_search_type::xor_displacement)
        , avg_partition_size(0)  // not partitioned
        , num_buckets(constants::invalid_num_buckets)
        , num_threads(1)
        , seed(constants::invalid_seed)
        , ram(static_cast<double>(constants::available_ram) * 0.75)
        , tmp_dir(constants::default_tmp_dirname)
        , secondary_sort(false)
        , dense_partitioning(false)
        , minimal(true)
        , verbose(true) {}

    double lambda;  // avg. bucket size
    double alpha;   // load factor
    pthash_search_type search;
    uint64_t avg_partition_size;
    uint64_t num_buckets;
    uint64_t num_threads;
    uint64_t seed;
    uint64_t ram;
    std::string tmp_dir;
    bool secondary_sort;
    bool dense_partitioning;
    bool minimal;
    bool verbose;
};

static uint64_t compute_num_buckets(const uint64_t num_keys, const uint64_t avg_bucket_size) {
    return std::ceil(static_cast<double>(num_keys) / avg_bucket_size);
}

static uint64_t compute_num_partitions(const uint64_t num_keys, const uint64_t avg_partition_size) {
    return std::ceil(static_cast<double>(num_keys) / avg_partition_size);
}

struct seed_runtime_error : public std::runtime_error {
    seed_runtime_error() : std::runtime_error("seed did not work") {}
};

#pragma pack(push, 4)
struct bucket_payload_pair {
    bucket_id_type bucket_id;
    uint64_t payload;

    bucket_payload_pair() {}
    bucket_payload_pair(bucket_id_type bucket_id, uint64_t payload)
        : bucket_id(bucket_id), payload(payload) {}

    bool operator<(bucket_payload_pair const& other) const {
        return (bucket_id < other.bucket_id) or
               (bucket_id == other.bucket_id and payload < other.payload);
    }
};
#pragma pack(pop)

struct bucket_t {
    bucket_t() : m_begin(nullptr), m_size(0) {}

    void init(uint64_t const* begin, bucket_size_type size) {
        m_begin = begin;
        m_size = size;
    }

    inline bucket_id_type id() const {
        return *m_begin;
    }

    inline uint64_t const* begin() const {
        return m_begin + 1;
    }

    inline uint64_t const* end() const {
        return m_begin + 1 + m_size;
    }

    inline bucket_size_type size() const {
        return m_size;
    }

private:
    uint64_t const* m_begin;
    bucket_size_type m_size;
};

template <typename PairsRandomAccessIterator>
struct payload_iterator {
    payload_iterator(PairsRandomAccessIterator const& iterator) : m_iterator(iterator) {}

    uint64_t operator*() const {
        return (*m_iterator).payload;
    }

    void operator++() {
        ++m_iterator;
    }

private:
    PairsRandomAccessIterator m_iterator;
};

template <typename Pairs, typename Merger>
void merge_single_block(Pairs const& pairs, Merger& merger, bool verbose) {
    progress_logger logger(pairs.size(), " == merged ", " pairs", verbose);

    bucket_size_type bucket_size = 1;
    uint64_t num_pairs = pairs.size();
    logger.log();
    for (uint64_t i = 1; i != num_pairs; ++i) {
        if (pairs[i].bucket_id == pairs[i - 1].bucket_id) {
            if (PTHASH_LIKELY(pairs[i].payload != pairs[i - 1].payload)) {
                ++bucket_size;
            } else {
                throw seed_runtime_error();
            }
        } else {
            merger.add(pairs[i - 1].bucket_id, bucket_size,
                       payload_iterator(pairs.begin() + i - bucket_size));
            bucket_size = 1;
        }
        logger.log();
    }

    // add the last bucket
    merger.add(pairs[num_pairs - 1].bucket_id, bucket_size,
               payload_iterator(pairs.end() - bucket_size));
    logger.finalize();
}

template <typename Pairs, typename Merger>
void merge_multiple_blocks(std::vector<Pairs> const& pairs_blocks, Merger& merger, bool verbose) {
    uint64_t num_pairs =
        std::accumulate(pairs_blocks.begin(), pairs_blocks.end(), static_cast<uint64_t>(0),
                        [](uint64_t sum, Pairs const& pairs) { return sum + pairs.size(); });
    progress_logger logger(num_pairs, " == merged ", " pairs", verbose);

    // input iterators and heap
    std::vector<typename Pairs::const_iterator> iterators;
    std::vector<uint32_t> idx_heap;
    iterators.reserve(pairs_blocks.size());
    idx_heap.reserve(pairs_blocks.size());

    // heap functions
    auto stdheap_idx_comparator = [&](uint32_t idxa, uint32_t idxb) {
        return !((*iterators[idxa]) < (*iterators[idxb]));
    };
    auto advance_heap_head = [&]() {
        auto idx = idx_heap[0];
        ++iterators[idx];
        if (PTHASH_LIKELY(iterators[idx] != pairs_blocks[idx].end())) {
            // percolate down the head
            uint64_t pos = 0;
            uint64_t size = idx_heap.size();
            while (2 * pos + 1 < size) {
                uint64_t i = 2 * pos + 1;
                if (i + 1 < size and stdheap_idx_comparator(idx_heap[i], idx_heap[i + 1])) ++i;
                if (stdheap_idx_comparator(idx_heap[i], idx_heap[pos])) break;
                std::swap(idx_heap[pos], idx_heap[i]);
                pos = i;
            }
        } else {
            std::pop_heap(idx_heap.begin(), idx_heap.end(), stdheap_idx_comparator);
            idx_heap.pop_back();
        }
    };

    // create the input iterators and the heap
    for (uint64_t i = 0; i != pairs_blocks.size(); ++i) {
        iterators.push_back(pairs_blocks[i].begin());
        idx_heap.push_back(i);
    }
    std::make_heap(idx_heap.begin(), idx_heap.end(), stdheap_idx_comparator);

    bucket_id_type bucket_id;
    std::vector<uint64_t> bucket_payloads;
    bucket_payloads.reserve(MAX_BUCKET_SIZE);

    // read the first pair
    {
        bucket_payload_pair pair = (*iterators[idx_heap[0]]);
        bucket_id = pair.bucket_id;
        bucket_payloads.push_back(pair.payload);
        advance_heap_head();
        logger.log();
    }

    // merge
    for (uint64_t i = 0; (PTHASH_LIKELY(idx_heap.size())); ++i, advance_heap_head()) {
        bucket_payload_pair pair = (*iterators[idx_heap[0]]);

        if (pair.bucket_id == bucket_id) {
            if (PTHASH_LIKELY(pair.payload != bucket_payloads.back())) {
                bucket_payloads.push_back(pair.payload);
            } else {
                throw seed_runtime_error();
            }
        } else {
            merger.add(bucket_id, bucket_payloads.size(), bucket_payloads.begin());
            bucket_id = pair.bucket_id;
            bucket_payloads.clear();
            bucket_payloads.push_back(pair.payload);
        }
        logger.log();
    }

    // add the last bucket
    merger.add(bucket_id, bucket_payloads.size(), bucket_payloads.begin());
    logger.finalize();
}

template <typename Pairs, typename Merger>
void merge(std::vector<Pairs> const& pairs_blocks, Merger& merger, bool verbose) {
    if (pairs_blocks.size() == 1) {
        merge_single_block(pairs_blocks[0], merger, verbose);
    } else {
        merge_multiple_blocks(pairs_blocks, merger, verbose);
    }
}

template <typename Taken, typename FreeSlots>
void fill_free_slots(Taken const& taken, const uint64_t num_keys, FreeSlots& free_slots,
                     const uint64_t table_size) {
    if (table_size <= num_keys) return;

    uint64_t next_used_slot = num_keys;
    uint64_t last_free_slot = 0, last_valid_free_slot = 0;

    auto last_free_slot_iter = taken.get_iterator_at(last_free_slot);
    auto next_used_slot_iter = taken.get_iterator_at(next_used_slot);

    while (true) {
        // find the next free slot (on the left)
        while (last_free_slot < num_keys && *last_free_slot_iter) {
            ++last_free_slot;
            ++last_free_slot_iter;
        }

        if (last_free_slot == num_keys) break;

        // fill with the last free slot (on the left) until I find a new used slot (on the right)
        // note: since I found a free slot on the left, there must be an used slot on the right
        assert(next_used_slot < table_size);
        while (!*next_used_slot_iter) {
            free_slots.emplace_back(last_free_slot);
            ++next_used_slot;
            ++next_used_slot_iter;
        }
        assert(next_used_slot < table_size);
        // fill the used slot (on the right) with the last free slot and advance all cursors
        free_slots.emplace_back(last_free_slot);
        last_valid_free_slot = last_free_slot;
        ++next_used_slot;
        ++last_free_slot;
        ++last_free_slot_iter;
        ++next_used_slot_iter;
    }
    // fill the tail with the last valid slot that I found
    while (next_used_slot != table_size) {
        free_slots.emplace_back(last_valid_free_slot);
        ++next_used_slot;
    }
    assert(next_used_slot == table_size);
}

template <typename RandomAccessIterator, typename Hasher>
struct hash_generator {
    hash_generator(RandomAccessIterator keys, uint64_t seed) : m_iterator(keys), m_seed(seed) {}

    inline auto operator*() {
        return Hasher::hash(*m_iterator, m_seed);
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

}  // namespace pthash