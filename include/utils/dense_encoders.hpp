#pragma once

#include <vector>
#include <cassert>
#include <thread>
#include <iomanip>
#include <sstream>

#include "encoders.hpp"

namespace pthash {

struct dense_encoder {};

template <typename Encoder>
struct dense_mono : dense_encoder {
    template <typename Iterator>
    void encode(Iterator begin,                                                            //
                const uint64_t num_partitions,                                             //
                const uint64_t num_buckets_per_partition, const uint64_t /*num_threads*/)  //
    {
        m_num_partitions = num_partitions;
        m_encoder.encode(begin, num_partitions * num_buckets_per_partition);
    }

    static std::string name() {
        return Encoder::name();
    }

    size_t size() const {
        return m_encoder.size();
    }

    size_t num_bits() const {
        return m_encoder.num_bits();
    }

    inline uint64_t access(const uint64_t partition, const uint64_t bucket) const {
        assert(m_num_partitions * bucket + partition < size());
        return m_encoder.access(m_num_partitions * bucket + partition);
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
        visitor.visit(t.m_num_partitions);
        visitor.visit(t.m_encoder);
    }

    uint64_t m_num_partitions;
    Encoder m_encoder;
};

template <typename Encoder>
struct dense_interleaved : dense_encoder {
    template <typename Iterator>
    void encode(Iterator begin,                                                        //
                const uint64_t num_partitions,                                         //
                const uint64_t num_buckets_per_partition, const uint64_t num_threads)  //
    {
        m_encoders.resize(num_buckets_per_partition);
        if (num_threads == 1) {
            for (uint64_t i = 0; i != num_buckets_per_partition; ++i) {
                m_encoders[i].encode(begin + i * num_partitions, num_partitions);
            }
        } else {
            auto exe = [&](uint64_t beginEncoder, uint64_t endEncoder) {
                for (; beginEncoder != endEncoder; ++beginEncoder) {
                    m_encoders[beginEncoder].encode(begin + beginEncoder * num_partitions,
                                                    num_partitions);
                }
            };

            std::vector<std::thread> threads(num_threads);
            uint64_t currentEncoder = 0;
            uint64_t i = 0;
            const uint64_t enc_per_thread =
                (num_buckets_per_partition + num_threads - 1) / num_threads;
            while (currentEncoder < num_buckets_per_partition) {
                uint64_t endEncoder = currentEncoder + enc_per_thread;
                if (endEncoder > num_buckets_per_partition) endEncoder = num_buckets_per_partition;
                threads[i] = std::thread(exe, currentEncoder, endEncoder);
                currentEncoder = endEncoder;
                i++;
            }
            for (auto& t : threads) {
                if (t.joinable()) t.join();
            }
        }
    }

    static std::string name() {
        return Encoder::name() + "-int";
    }

    inline uint64_t access(const uint64_t partition, const uint64_t bucket) const {
        assert(bucket < m_encoders.size());
        return m_encoders[bucket].access(partition);
    }

    uint64_t num_bits() const {
        uint64_t sum = 8 * sizeof(uint64_t);  // for std::vector size
        for (auto const& e : m_encoders) sum += e.num_bits();
        return sum;
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
        visitor.visit(t.m_encoders);
    }

    std::vector<Encoder> m_encoders;
};

typedef dense_mono<compact> C_mono;
typedef dense_mono<dictionary> D_mono;
typedef dense_mono<rice> R_mono;
typedef dense_mono<elias_fano> EF_mono;

typedef dense_interleaved<compact> C_int;
typedef dense_interleaved<dictionary> D_int;
typedef dense_interleaved<rice> R_int;

}  // namespace pthash