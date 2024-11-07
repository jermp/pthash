#pragma once

#include "include/utils/bucketers.hpp"
#include "include/builders/util.hpp"
#include "include/builders/internal_memory_builder_single_phf.hpp"
#include "include/builders/external_memory_builder_single_phf.hpp"

namespace pthash {

template <typename Hasher,    //
          typename Bucketer,  //
          typename Encoder,   //
          bool Minimal,
          pthash_search_type Search>
struct single_phf  //
{
    static_assert(
        !std::is_base_of<dense_encoder, Encoder>::value,
        "Dense encoders are only valid for dense_partitioned_phf. Select another encoder.");
    typedef Encoder encoder_type;
    static constexpr bool minimal = Minimal;

    template <typename Iterator>
    build_timings build_in_internal_memory(Iterator keys, const uint64_t num_keys,
                                           build_configuration const& config) {
        build_configuration build_config = set_build_configuration(config);
        internal_memory_builder_single_phf<Hasher, Bucketer> builder;
        auto timings = builder.build_from_keys(keys, num_keys, build_config);
        timings.encoding_microseconds = build(builder, build_config);
        return timings;
    }

    template <typename Iterator>
    build_timings build_in_external_memory(Iterator keys, const uint64_t num_keys,
                                           build_configuration const& config) {
        build_configuration build_config = set_build_configuration(config);
        external_memory_builder_single_phf<Hasher, Bucketer> builder;
        auto timings = builder.build_from_keys(keys, num_keys, build_config);
        timings.encoding_microseconds = build(builder, build_config);
        return timings;
    }

    template <typename Builder>
    uint64_t build(Builder const& builder, build_configuration const& config) {
        auto start = clock_type::now();

        if (Minimal != config.minimal) {
            throw std::runtime_error(  //
                "template parameter 'Minimal' must be equal to config.minimal");
        }
        if (Search != config.search) {
            throw std::runtime_error(  //
                "template parameter 'Search' must be equal to config.search");
        }

        m_seed = builder.seed();
        m_num_keys = builder.num_keys();
        m_table_size = builder.table_size();
        m_M_128 = fastmod::computeM_u64(m_table_size);
        m_M_64 = fastmod::computeM_u32(m_table_size);
        m_bucketer = builder.bucketer();
        m_pilots.encode(builder.pilots().data(), m_bucketer.num_buckets());
        if (Minimal and m_num_keys < m_table_size) {
            assert(builder.free_slots().size() == m_table_size - m_num_keys);
            m_free_slots.encode(builder.free_slots().begin(), m_table_size - m_num_keys);
        }
        auto stop = clock_type::now();

        return to_microseconds(stop - start);
    }

    template <typename T>
    uint64_t operator()(T const& key) const {
        auto hash = Hasher::hash(key, m_seed);
        return position(hash);
    }

    uint64_t position(typename Hasher::hash_type hash) const {
        const uint64_t bucket = m_bucketer.bucket(hash.first());
        const uint64_t pilot = m_pilots.access(bucket);

        uint64_t p = 0;
        if constexpr (Search == pthash_search_type::xor_displacement) {
            /* xor displacement */
            const uint64_t hashed_pilot = default_hash64(pilot, m_seed);
            p = fastmod::fastmod_u64(hash.second() ^ hashed_pilot, m_M_128, m_table_size);
        } else {
            /* additive displacement */
            const uint64_t s = fastmod::fastdiv_u32(pilot, m_M_64);
            p = fastmod::fastmod_u32(((hash64(hash.second() + s).mix()) >> 33) + pilot, m_M_64,
                                     m_table_size);
        }

        if constexpr (Minimal) {
            if (PTHASH_LIKELY(p < num_keys())) return p;
            return m_free_slots.access(p - num_keys());
        }

        return p;
    }

    uint64_t num_bits_for_pilots() const {
        return 8 * (sizeof(m_seed) + sizeof(m_num_keys) + sizeof(m_table_size) + sizeof(m_M_64) +
                    sizeof(m_M_128)) +
               m_pilots.num_bits();
    }

    uint64_t num_bits_for_mapper() const {
        return m_bucketer.num_bits() + m_free_slots.num_bytes() * 8;
    }

    uint64_t num_bits() const {
        return num_bits_for_pilots() + num_bits_for_mapper();
    }

    uint64_t num_keys() const {
        return m_num_keys;
    }

    uint64_t table_size() const {
        return m_table_size;
    }

    uint64_t seed() const {
        return m_seed;
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
        visitor.visit(t.m_M_128);
        visitor.visit(t.m_M_64);
        visitor.visit(t.m_bucketer);
        visitor.visit(t.m_pilots);
        visitor.visit(t.m_free_slots);
    }

    static build_configuration set_build_configuration(build_configuration const& config) {
        build_configuration build_config = config;
        if (config.minimal != Minimal) {
            if (config.verbose) {
                std::cout << "setting config.verbose = " << (Minimal ? "true" : "false")
                          << std::endl;
            }
            build_config.minimal = Minimal;
        }
        if (config.search != Search) {
            if (config.verbose) { std::cout << "setting config.search = " << Search << std::endl; }
            build_config.search = Search;
        }
        return build_config;
    }

    uint64_t m_seed;
    uint64_t m_num_keys;
    uint64_t m_table_size;
    __uint128_t m_M_128;
    uint64_t m_M_64;
    Bucketer m_bucketer;
    Encoder m_pilots;
    bits::elias_fano<false, false> m_free_slots;
};

}  // namespace pthash