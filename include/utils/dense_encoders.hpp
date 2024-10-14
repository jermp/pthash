#pragma once

#include <vector>
#include <cassert>
#include <thread>
#include "encoders.hpp"

namespace pthash {

template <typename Encoder>
struct diff {
    template <typename Iterator>
    void encode(Iterator begin, const uint64_t size, const uint64_t increment) {
        m_increment = increment;
        std::vector<uint64_t> diff_values;
        diff_values.reserve(size);
        int64_t expected = 0;
        for (uint64_t i = 0; i != size; ++i, ++begin) {
            int64_t to_encode = *begin - expected;
            uint64_t abs_to_encode = abs(to_encode);
            diff_values.push_back((abs_to_encode << 1) | uint64_t(to_encode > 0));
            expected += increment;
        }
        m_encoder.encode(diff_values.begin(), size);
    }

    size_t size() const {
        return m_encoder.size();
    }

    size_t num_bits() const {
        return sizeof(m_increment) + m_encoder.num_bits();
    }

    inline uint64_t access(uint64_t i) const {
        const uint64_t value = m_encoder.access(i);
        const uint64_t expected = i * m_increment;
        int64_t diff = ((value & 1) * 2 - 1) * int64_t(value >> 1);
        return expected + diff;
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
        visitor.visit(t.m_increment);
        visitor.visit(t.m_encoder);
    }
    uint64_t m_increment;
    Encoder m_encoder;
};

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
        return "mono-" + Encoder::name();
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
        return "inter-" + Encoder::name();
    }

    inline uint64_t access(const uint64_t partition, const uint64_t bucket) const {
        assert(bucket < m_encoders.size());
        return m_encoders[bucket].access(partition);
    }

    uint64_t num_bits() const {
        uint64_t sum = 0;
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

template <typename Front, typename Back, uint64_t numerator = 1, uint64_t denominator = 3>
struct dense_dual : dense_encoder {
    template <typename Iterator>
    void encode(Iterator begin,                                                        //
                const uint64_t num_partitions,                                         //
                const uint64_t num_buckets_per_partition, const uint64_t num_threads)  //
    {
        m_front_size = num_buckets_per_partition * (static_cast<double>(numerator) / denominator);
        if (num_threads == 1) {
            if (m_front_size > 0) m_front.encode(begin, num_partitions, m_front_size, 1);
            if (num_buckets_per_partition - m_front_size > 0)
                m_back.encode(begin + m_front_size * num_partitions, num_partitions,
                              num_buckets_per_partition - m_front_size, 1);
        } else {
            uint64_t m_front_threads =
                (num_threads * m_front_size + num_buckets_per_partition - 1) /
                num_buckets_per_partition;
            auto exe = [&]() {
                if (m_front_size > 0)
                    m_front.encode(begin, num_partitions, m_front_size, m_front_threads);
            };
            std::thread frontThread = std::thread(exe);
            if (num_buckets_per_partition - m_front_size > 0)
                m_back.encode(begin + m_front_size * num_partitions, num_partitions,
                              num_buckets_per_partition - m_front_size,
                              num_threads - m_front_threads);
            if (frontThread.joinable()) frontThread.join();
        }
    }

    static std::string name() {
        return Front::name() + "-" + Back::name() + "-" +
               std::to_string(static_cast<double>(numerator) / denominator);
    }

    size_t num_bits() const {
        return sizeof(m_front_size) * 8 + m_front.num_bits() + m_back.num_bits();
    }

    uint64_t access(uint64_t i) const {
        if (i < m_front.size()) return m_front.access(i);
        return m_back.access(i - m_front.size());
    }

    inline uint64_t access(const uint64_t partition, const uint64_t bucket) const {
        if (bucket < m_front_size) return m_front.access(partition, bucket);
        return m_back.access(partition, bucket - m_front_size);
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
        visitor.visit(t.m_front_size);
        visitor.visit(t.m_front);
        visitor.visit(t.m_back);
    }
    uint64_t m_front_size;
    Front m_front;
    Back m_back;
};

typedef dense_mono<rice> mono_R;
typedef dense_interleaved<rice> inter_R;
typedef dense_mono<compact> mono_C;
typedef dense_interleaved<compact> inter_C;
typedef dense_mono<dictionary> mono_D;
typedef dense_interleaved<dictionary> inter_D;
typedef dense_mono<elias_fano> mono_EF;
typedef dense_interleaved<elias_fano> inter_EF;

/* dual_interleaved encoders */
typedef dense_dual<mono_C, mono_R, 1, 3> mono_C_mono_R;
typedef dense_dual<inter_C, inter_R, 1, 3> inter_C_inter_R;
typedef dense_dual<mono_D, mono_R, 1, 3> mono_D_mono_R;
typedef dense_dual<inter_D, inter_R, 1, 3> inter_D_inter_R;

}  // namespace pthash