#pragma once

#include "util.hpp"
#include "compact_vector.hpp"
#include "elias_fano.hpp"
#include "ranked_sequence.hpp"
#include "rice_sequence.hpp"

#include <vector>
#include <cassert>

namespace pthash {

struct compact {
    template <typename Iterator>
    void encode(Iterator begin, const uint64_t n) {
        if (n == 0) return;
        m_values.build(begin, n);
    }

    static std::string name() {
        return "C";
    }

    uint64_t size() const {
        return m_values.size();
    }

    uint64_t num_bits() const {
        return m_values.num_bytes() * 8;
    }

    uint64_t access(uint64_t i) const {
        return m_values.access(i);
    }

    template <typename Visitor>
    void visit(Visitor& visitor) const {
        visitor.visit(m_values);
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visitor.visit(m_values);
    }

private:
    bits::compact_vector m_values;
};

struct partitioned_compact {
    static const uint64_t partition_size = 256;
    static_assert(partition_size > 0);

    template <typename Iterator>
    void encode(Iterator begin, const uint64_t n) {
        if (n == 0) return;
        uint64_t num_partitions = (n + partition_size - 1) / partition_size;
        bits::bit_vector::builder bvb;
        bvb.reserve(32 * n);
        m_bits_per_value.reserve(num_partitions + 1);
        m_bits_per_value.push_back(0);
        for (uint64_t i = 0, begin_partition = 0; i != num_partitions; ++i) {
            uint64_t end_partition = begin_partition + partition_size;
            if (end_partition > n) end_partition = n;
            uint64_t max_value = *std::max_element(begin + begin_partition, begin + end_partition);
            uint64_t num_bits = (max_value == 0) ? 1 : std::ceil(std::log2(max_value + 1));
            assert(num_bits > 0);

            // std::cout << i << ": " << num_bits << '\n';
            // for (uint64_t k = begin_partition; k != end_partition; ++k) {
            //     uint64_t num_bits_val = (begin[k] == 0) ? 1 : std::ceil(std::log2(begin[k] +
            //     1)); std::cout << "  " << num_bits_val << '/' << num_bits << '\n';
            // }

            for (uint64_t k = begin_partition; k != end_partition; ++k) {
                bvb.append_bits(*(begin + k), num_bits);
            }
            assert(m_bits_per_value.back() + num_bits < (1ULL << 32));
            m_bits_per_value.push_back(m_bits_per_value.back() + num_bits);
            begin_partition = end_partition;
        }
        bvb.build(m_values);
    }

    static std::string name() {
        return "PC";
    }

    uint64_t size() const {
        return m_size;
    }

    uint64_t num_bits() const {
        return (sizeof(m_size) + essentials::vec_bytes(m_bits_per_value) + m_values.num_bytes()) *
               8;
    }

    uint64_t access(uint64_t i) const {
        uint64_t partition = i / partition_size;
        uint64_t offset = i % partition_size;
        uint64_t num_bits = m_bits_per_value[partition + 1] - m_bits_per_value[partition];
        uint64_t position = m_bits_per_value[partition] * partition_size + offset * num_bits;
        return m_values.get_bits(position, num_bits);
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
        visitor.visit(t.m_size);
        visitor.visit(t.m_bits_per_value);
        visitor.visit(t.m_values);
    }

    uint64_t m_size;
    std::vector<uint32_t> m_bits_per_value;
    bits::bit_vector m_values;
};

struct dictionary {
    template <typename Iterator>
    void encode(Iterator begin, const uint64_t n) {
        if (n == 0) return;
        m_values.encode(begin, n);
    }

    static std::string name() {
        return "D";
    }

    uint64_t size() const {
        return m_values.size();
    }

    uint64_t num_bits() const {
        return m_values.num_bytes() * 8;
    }

    uint64_t access(uint64_t i) const {
        return m_values.access(i);
    }

    template <typename Visitor>
    void visit(Visitor& visitor) const {
        visitor.visit(m_values);
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visitor.visit(m_values);
    }

private:
    bits::ranked_sequence m_values;
};

struct elias_fano {
    template <typename Iterator>
    void encode(Iterator begin, const uint64_t n) {
        if (n == 0) return;
        m_values.encode(begin, n);
    }

    static std::string name() {
        return "EF";
    }

    uint64_t size() const {
        return m_values.size();
    }

    uint64_t num_bits() const {
        return m_values.num_bytes() * 8;
    }

    uint64_t access(uint64_t i) const {
        assert(i + 1 < m_values.size());
        return m_values.diff(i);
    }

    template <typename Visitor>
    void visit(Visitor& visitor) const {
        visitor.visit(m_values);
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visitor.visit(m_values);
    }

private:
    bits::elias_fano<false, true> m_values;
};

struct rice {
    template <typename Iterator>
    void encode(Iterator begin, const uint64_t n) {
        if (n == 0) return;
        m_values.encode(begin, n);
    }

    static std::string name() {
        return "R";
    }

    uint64_t size() const {
        return m_values.size();
    }

    uint64_t num_bits() const {
        return 8 * m_values.num_bytes();
    }

    uint64_t access(uint64_t i) const {
        return m_values.access(i);
    }

    template <typename Visitor>
    void visit(Visitor& visitor) const {
        visitor.visit(m_values);
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visitor.visit(m_values);
    }

private:
    bits::rice_sequence<> m_values;
};

template <typename Front, typename Back>
struct dual {
    template <typename Iterator>
    void encode(Iterator begin, const uint64_t n) {
        if (n == 0) return;
        uint64_t front_size = n * constants::b;
        m_front.encode(begin, front_size);
        m_back.encode(begin + front_size, n - front_size);
    }

    static std::string name() {
        return Front::name() + "-" + Back::name();
    }

    uint64_t num_bits() const {
        return m_front.num_bits() + m_back.num_bits();
    }

    uint64_t access(uint64_t i) const {
        if (i < m_front.size()) return m_front.access(i);
        return m_back.access(i - m_front.size());
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
        visitor.visit(t.m_front);
        visitor.visit(t.m_back);
    }

    Front m_front;
    Back m_back;
};

/* dual encoders */
typedef dual<rice, rice> rice_rice;
typedef dual<compact, compact> compact_compact;
typedef dual<dictionary, dictionary> dictionary_dictionary;

}  // namespace pthash