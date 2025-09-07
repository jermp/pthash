#pragma once

#include <thread>

#include "single_phf.hpp"
#include "builders/internal_memory_builder_partitioned_phf.hpp"
#include "builders/external_memory_builder_partitioned_phf.hpp"

namespace pthash {

template <typename Hasher, typename Bucketer, typename Encoder, bool Minimal>
struct partitioned_phf  //
{
    static_assert(
        !std::is_base_of<dense_encoder, Encoder>::value,
        "Dense encoders are only valid for dense_partitioned_phf. Select another encoder.");

private:
    struct partition {
        template <typename Visitor>
        void visit(Visitor& visitor) const {
            visit_impl(visitor, *this);
        }

        template <typename Visitor>
        void visit(Visitor& visitor) {
            visit_impl(visitor, *this);
        }

        uint64_t offset;
        single_phf<Hasher, Bucketer, Encoder, Minimal> f;

    private:
        template <typename Visitor, typename T>
        static void visit_impl(Visitor& visitor, T&& t) {
            visitor.visit(t.offset);
            visitor.visit(t.f);
        }
    };

    static build_configuration set_build_configuration(build_configuration const& config) {
        build_configuration build_config = config;
        if (config.minimal != Minimal) {
            if (config.verbose) {
                std::cout << "setting config.verbose = " << (Minimal ? "true" : "false")
                          << std::endl;
            }
            build_config.minimal = Minimal;
        }
        if (config.dense_partitioning == true) {
            if (config.verbose) {
                std::cout << "setting config.dense_partitioning = false" << std::endl;
            }
            build_config.dense_partitioning = false;
        }
        return build_config;
    }

public:
    typedef Encoder encoder_type;
    static constexpr bool minimal = Minimal;

    template <typename Iterator>
    build_timings build_in_internal_memory(Iterator keys, const uint64_t num_keys,
                                           build_configuration const& config) {
        build_configuration build_config = set_build_configuration(config);
        internal_memory_builder_partitioned_phf<Hasher, Bucketer> builder;
        auto timings = builder.build_from_keys(keys, num_keys, build_config);
        timings.encoding_microseconds = build(builder, build_config);
        return timings;
    }

    template <typename Iterator>
    build_timings build_in_external_memory(Iterator keys, const uint64_t num_keys,
                                           build_configuration const& config) {
        build_configuration build_config = set_build_configuration(config);
        external_memory_builder_partitioned_phf<Hasher, Bucketer> builder;
        auto timings = builder.build_from_keys(keys, num_keys, build_config);
        timings.encoding_microseconds = build(builder, build_config);
        return timings;
    }

    template <typename Builder>
    uint64_t build(Builder& builder, build_configuration const& config) {
        auto start = clock_type::now();

        if (Minimal != config.minimal) {
            throw std::runtime_error(  //
                "template parameter 'Minimal' must be equal to config.minimal");
        }

        uint64_t num_partitions = builder.num_partitions();
        m_seed = builder.seed();
        m_num_keys = builder.num_keys();
        m_table_size = builder.table_size();
        m_partitioner = builder.bucketer();
        m_partitions.resize(num_partitions);

        auto const& offsets = builder.offsets();
        auto const& builders = builder.builders();
        const uint64_t num_threads = config.num_threads;

        if (num_threads > 1) {
            std::vector<std::thread> threads(num_threads);
            auto exe = [&](uint64_t begin, uint64_t end) {
                for (; begin != end; ++begin) {
                    m_partitions[begin].offset = offsets[begin];
                    m_partitions[begin].f.build(builders[begin], config);
                }
            };

            const uint64_t num_partitions_per_thread =
                (num_partitions + num_threads - 1) / num_threads;
            for (uint64_t t = 0, begin = 0; t != num_threads; ++t) {
                uint64_t end = begin + num_partitions_per_thread;
                if (end > num_partitions) end = num_partitions;
                threads[t] = std::thread(exe, begin, end);
                begin = end;
            }

            for (auto& t : threads) {
                if (t.joinable()) t.join();
            }
        } else {
            for (uint64_t i = 0; i != num_partitions; ++i) {
                m_partitions[i].offset = offsets[i];
                m_partitions[i].f.build(builders[i], config);
            }
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
        auto b = m_partitioner.bucket(hash.mix());
        auto const& p = m_partitions[b];
        return p.offset + p.f.position(hash);
    }

    uint64_t num_bits_for_pilots() const {
        uint64_t bits = 8 * (sizeof(m_seed) + sizeof(m_num_keys) + sizeof(m_table_size) +
                             sizeof(uint64_t)  // for std::vector::size
                             ) +
                        m_partitioner.num_bits();
        for (auto const& p : m_partitions) bits += 8 * sizeof(p.offset) + p.f.num_bits_for_pilots();
        return bits;
    }

    uint64_t num_bits_for_mapper() const {
        uint64_t bits = 0;
        for (auto const& p : m_partitions) bits += p.f.num_bits_for_mapper();
        return bits;
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
        visitor.visit(t.m_partitioner);
        visitor.visit(t.m_partitions);
    }

    uint64_t m_seed;
    uint64_t m_num_keys;
    uint64_t m_table_size;
    range_bucketer m_partitioner;
    std::vector<partition> m_partitions;
};

}  // namespace pthash