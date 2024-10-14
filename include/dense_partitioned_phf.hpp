#pragma once

#include "builders/internal_memory_builder_partitioned_phf.hpp"

namespace pthash {

template <typename Hasher, typename Bucketer, typename Encoder, bool NeedsFreeArray,
          pthash_search_type Search>
struct dense_partitioned_phf {
    static_assert(std::is_base_of<dense_encoder, Encoder>::value,
                  "Needs a dense encoder for dense partitioned PTHash. Select another encoder.");
    typedef Encoder encoder_type;
    static constexpr bool needsFreeArray = NeedsFreeArray;

    static constexpr bool minimal = true;  // ToDO

    template <typename Iterator>
    build_timings build_in_internal_memory(Iterator keys, const uint64_t num_keys,
                                           build_configuration const& config) {
        assert(Search == config.search);
        assert(config.dense_partitioning == true);
        assert(config.avg_partition_size < 10000);  // Unlike partitioned, must use small partitions
        internal_memory_builder_partitioned_phf<Hasher, Bucketer> builder;
        auto timings = builder.build_from_keys(keys, num_keys, config);
        timings.encoding_microseconds = build(builder, config);
        return timings;
    }

    template <typename Builder>
    double build(Builder& builder, build_configuration const& config)  //
    {
        auto start = clock_type::now();

        const uint64_t num_partitions = builder.num_partitions();
        const uint64_t num_buckets_per_partition = builder.num_buckets_per_partition();

        m_seed = builder.seed();
        m_num_keys = builder.num_keys();
        m_table_size = builder.table_size();
        m_partitioner = builder.bucketer();

        auto const& offsets = builder.offsets();
        auto const& builders = builder.builders();
        m_bucketer = builders.front().bucketer();

        const uint64_t increment = m_table_size / num_partitions;
        m_offsets.encode(offsets.begin(), offsets.size(), increment);
        m_pilots.encode(builder.interleaving_pilots_iterator_begin(), num_partitions,
                        num_buckets_per_partition, config.num_threads);

        if constexpr (needsFreeArray) {
            assert(builder.free_slots().size() == m_table_size - m_num_keys);
            m_free_slots.encode(builder.free_slots().begin(), m_table_size - m_num_keys);
        }

        auto stop = clock_type::now();

        return to_microseconds(stop - start);
    }

    template <typename T>
    uint64_t operator()(T const& key) const  //
    {
        auto hash = Hasher::hash(key, m_seed);
        const uint64_t partition = m_partitioner.bucket(hash.mix());
        const uint64_t partition_offset = m_offsets.access(partition);
        const uint64_t partition_size = m_offsets.access(partition + 1) - partition_offset;
        const uint64_t p = partition_offset + position(hash, partition, partition_size);
        if constexpr (needsFreeArray) {
            if (PTHASH_LIKELY(p < num_keys())) return p;
            return m_free_slots.access(p - num_keys());
        }
        return p;
    }

    uint64_t position(typename Hasher::hash_type hash,      //
                      const uint64_t partition,             //
                      const uint64_t partition_size) const  //
    {
        const uint64_t bucket = m_bucketer.bucket(hash.first());
        const uint64_t pilot = m_pilots.access(partition, bucket);
        if constexpr (Search == pthash_search_type::xor_displacement) {
            /* xor displacement */
            const __uint128_t M = fastmod::computeM_u64(partition_size);
            const uint64_t hashed_pilot = default_hash64(pilot, m_seed);
            return fastmod::fastmod_u64(hash.second() ^ hashed_pilot, M, partition_size);
        } else {
            /* additive displacement */
            const uint64_t M = fastmod::computeM_u32(partition_size);
            const uint64_t s = fastmod::fastdiv_u32(pilot, M);
            return fastmod::fastmod_u32(((hash64(hash.second() + s).mix()) >> 33) + pilot, M,
                                        partition_size);
        }
    }

    size_t num_bits_for_pilots() const {
        return 8 * (sizeof(m_seed) + sizeof(m_num_keys) + sizeof(m_table_size)) +
               m_pilots.num_bits();
    }

    size_t num_bits_for_mapper() const {
        return m_partitioner.num_bits() + m_bucketer.num_bits() + m_offsets.num_bits() +
               (needsFreeArray ? m_free_slots.num_bytes() * 8 : 0);
    }

    size_t num_bits() const {
        return num_bits_for_pilots() + num_bits_for_mapper();
    }

    inline uint64_t num_keys() const {
        return m_num_keys;
    }

    inline uint64_t table_size() const {
        return m_table_size;
    }

    template <typename Visitor>
    void visit(Visitor& visitor) const {
        visit_impl(visitor, *this);
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visit_impl(visitor, *this);
    }

private:
    template <typename Visitor, typename T>
    static void visit_impl(Visitor& visitor, T&& t) {
        visitor.visit(t.m_seed);
        visitor.visit(t.m_num_keys);
        visitor.visit(t.m_table_size);
        visitor.visit(t.m_partitioner);
        visitor.visit(t.m_bucketer);
        visitor.visit(t.m_pilots);
        visitor.visit(t.m_offsets);
        if (needsFreeArray) visitor.visit(t.m_free_slots);
    }

    uint64_t m_seed;
    uint64_t m_num_keys;
    uint64_t m_table_size;
    range_bucketer m_partitioner;
    Bucketer m_bucketer;
    Encoder m_pilots;
    diff<compact> m_offsets;
    bits::elias_fano<false, false> m_free_slots;
};

template <typename Hasher, typename Encoder>
using phobic =
    dense_partitioned_phf<Hasher, table_bucketer<opt_bucketer>, dense_interleaved<Encoder>, false,
                          pthash_search_type::add_displacement>;

}  // namespace pthash