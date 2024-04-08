#pragma once

#include <vector>
#include <cassert>
#include <thread>

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
    void visit(Visitor& visitor) {
        visitor.visit(m_increment);
        visitor.visit(m_encoder);
    }

private:
    uint64_t m_increment;
    Encoder m_encoder;
};

template <typename Encoder>
struct mono_interleaved {
    template <typename Iterator>
    void encode(Iterator begin,                            //
                const uint64_t num_partitions,             //
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
    void visit(Visitor& visitor) {
        visitor.visit(m_num_partitions);
        visitor.visit(m_encoder);
    }

private:
    uint64_t m_num_partitions;
    Encoder m_encoder;
};

template <typename Encoder>
struct multi_interleaved {
    template <typename Iterator>
    void encode(Iterator begin,                            //
                const uint64_t num_partitions,             //
                const uint64_t num_buckets_per_partition, const uint64_t num_threads)  //
    {
        m_encoders.resize(num_buckets_per_partition);
        if(num_threads==1) {
            for (uint64_t i = 0; i != num_buckets_per_partition; ++i) {
                m_encoders[i].encode(begin + i * num_partitions, num_partitions);
            }
        } else {
            auto exe = [&](uint64_t beginEncoder, uint64_t endEncoder) {
                for (; beginEncoder != endEncoder; ++beginEncoder) {
                    m_encoders[beginEncoder].encode(begin + beginEncoder * num_partitions, num_partitions);
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
        return "multi-" + Encoder::name();
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
    void visit(Visitor& visitor) {
        for (auto& e : m_encoders) e.visit(visitor);
    }

private:
    std::vector<Encoder> m_encoders;
};

template <typename Front, typename Back, uint64_t numerator = 1, uint64_t denominator = 3>
struct dual_interleaved {
    template <typename Iterator>
    void encode(Iterator begin,                            //
                const uint64_t num_partitions,             //
                const uint64_t num_buckets_per_partition, const uint64_t num_threads)  //
    {
        m_front_size = num_buckets_per_partition * (static_cast<double>(numerator) / denominator);
        if(num_threads == 1) {
            if(m_front_size > 0) m_front.encode(begin, num_partitions, m_front_size, 1);
            if(num_buckets_per_partition - m_front_size > 0) m_back.encode(begin + m_front_size * num_partitions, num_partitions,
                          num_buckets_per_partition - m_front_size, 1);
        } else {
            uint64_t m_front_threads =
                (num_threads * m_front_size + num_buckets_per_partition - 1) /
                num_buckets_per_partition;
            auto exe = [&]() {
                if(m_front_size > 0) m_front.encode(begin, num_partitions, m_front_size, m_front_threads);
            };
            std::thread frontThread = std::thread(exe);
            if(num_buckets_per_partition - m_front_size > 0) m_back.encode(begin + m_front_size * num_partitions, num_partitions,
                          num_buckets_per_partition - m_front_size, num_threads - m_front_threads);
            if (frontThread.joinable()) frontThread.join();
        }
    }

    static std::string name() {
        return Front::name() + "-" + Back::name() + "-" + std::to_string(static_cast<double>(numerator)/denominator);
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
    void visit(Visitor& visitor) {
        visitor.visit(m_front_size);
        visitor.visit(m_front);
        visitor.visit(m_back);
    }

private:
    uint64_t m_front_size;
    Front m_front;
    Back m_back;
};

typedef mono_interleaved<rice> mono_R;
typedef multi_interleaved<rice> multi_R;
typedef mono_interleaved<compact> mono_C;
typedef multi_interleaved<compact> multi_C;
typedef mono_interleaved<dictionary> mono_D;
typedef multi_interleaved<dictionary> multi_D;
typedef mono_interleaved<elias_fano> mono_EF;
typedef multi_interleaved<elias_fano> multi_EF;

/* dual_interleaved encoders */
typedef dual_interleaved<mono_C, mono_R, 1, 3> mono_C_mono_R;
typedef dual_interleaved<multi_C, multi_R, 1, 3> multi_C_multi_R;
typedef dual_interleaved<mono_D, mono_R, 1, 3> mono_D_mono_R;
typedef dual_interleaved<multi_D, multi_R, 1, 3> multi_D_multi_R;

}  // namespace pthash