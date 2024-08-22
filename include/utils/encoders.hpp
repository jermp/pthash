#pragma once

#include "external/bits/include/compact_vector.hpp"
#include "external/bits/include/elias_fano.hpp"

#include <vector>
#include <unordered_map>
#include <cassert>

namespace pthash {

struct compact {
    template <typename Iterator>
    void encode(Iterator begin, uint64_t n) {
        m_values.build(begin, n);
    }

    static std::string name() {
        return "compact";
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
    void encode(Iterator begin, uint64_t n) {
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
        return "partitioned_compact";
    }

    uint64_t size() const {
        return m_size;
    }

    uint64_t num_bits() const {
        return (sizeof(m_size) + m_bits_per_value.size() * sizeof(m_bits_per_value.front()) +
                m_values.num_bytes()) *
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

template <typename Iterator>
std::pair<std::vector<uint64_t>, std::vector<uint64_t>> compute_ranks_and_dictionary(Iterator begin,
                                                                                     uint64_t n) {
    // accumulate frequencies
    std::unordered_map<uint64_t, uint64_t> distinct;
    for (auto it = begin, end = begin + n; it != end; ++it) {
        auto find_it = distinct.find(*it);
        if (find_it != distinct.end()) {  // found
            (*find_it).second += 1;
        } else {
            distinct[*it] = 1;
        }
    }
    std::vector<std::pair<uint64_t, uint64_t>> vec;
    vec.reserve(distinct.size());
    for (auto p : distinct) vec.emplace_back(p.first, p.second);
    std::sort(vec.begin(), vec.end(),
              [](auto const& x, auto const& y) { return x.second > y.second; });
    distinct.clear();
    // assign codewords by non-increasing frequency
    std::vector<uint64_t> dict;
    dict.reserve(distinct.size());
    for (uint64_t i = 0; i != vec.size(); ++i) {
        auto p = vec[i];
        distinct.insert({p.first, i});
        dict.push_back(p.first);
    }

    std::vector<uint64_t> ranks;
    ranks.reserve(n);
    for (auto it = begin, end = begin + n; it != end; ++it) ranks.push_back(distinct[*it]);
    return {ranks, dict};
}

struct dictionary {
    template <typename Iterator>
    void encode(Iterator begin, uint64_t n) {
        auto [ranks, dict] = compute_ranks_and_dictionary(begin, n);
        m_ranks.build(ranks.begin(), ranks.size());
        m_dict.build(dict.begin(), dict.size());
    }

    static std::string name() {
        return "dictionary";
    }

    uint64_t size() const {
        return m_ranks.size();
    }

    uint64_t num_bits() const {
        return (m_ranks.num_bytes() + m_dict.num_bytes()) * 8;
    }

    uint64_t access(uint64_t i) const {
        uint64_t rank = m_ranks.access(i);
        return m_dict.access(rank);
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
        visitor.visit(t.m_ranks);
        visitor.visit(t.m_dict);
    }
    bits::compact_vector m_ranks;
    bits::compact_vector m_dict;
};

struct elias_fano {
    template <typename Iterator>
    void encode(Iterator begin, uint64_t n) {
        m_values.encode(begin, n);
    }

    static std::string name() {
        return "elias_fano";
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

struct sdc_sequence {
    sdc_sequence() : m_size(0) {}

    template <typename Iterator>
    void build(Iterator begin, uint64_t n) {
        m_size = n;
        auto start = begin;
        uint64_t bits = 0;
        for (uint64_t i = 0; i < n; ++i, ++start) bits += std::floor(std::log2(*start + 1));
        bits::bit_vector::builder bvb(bits);
        std::vector<uint64_t> lengths;
        lengths.reserve(n + 1);
        uint64_t pos = 0;
        for (uint64_t i = 0; i < n; ++i, ++begin) {
            auto v = *begin;
            uint64_t len = std::floor(std::log2(v + 1));
            assert(len <= 64);
            uint64_t cw = v + 1 - (uint64_t(1) << len);
            if (len > 0) bvb.set_bits(pos, cw, len);
            lengths.push_back(pos);
            pos += len;
        }
        assert(pos == bits);
        lengths.push_back(pos);
        bvb.build(m_codewords);
        m_index.encode(lengths.begin(), lengths.size());
    }

    inline uint64_t access(uint64_t i) const {
        assert(i < size());
        uint64_t pos = m_index.access(i);
        uint64_t len = m_index.access(i + 1) - pos;
        assert(len < 64);
        uint64_t cw = m_codewords.get_bits(pos, len);
        uint64_t value = cw + (uint64_t(1) << len) - 1;
        return value;
    }

    uint64_t size() const {
        return m_size;
    }

    uint64_t num_bytes() const {
        return sizeof(m_size) + m_codewords.num_bytes() + m_index.num_bytes();
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visitor.visit(m_size);
        visitor.visit(m_codewords);
        visitor.visit(m_index);
    }

private:
    uint64_t m_size;
    bits::bit_vector m_codewords;
    bits::elias_fano<false, false> m_index;
};

struct sdc {
    template <typename Iterator>
    void encode(Iterator begin, uint64_t n) {
        auto [ranks, dict] = compute_ranks_and_dictionary(begin, n);
        m_ranks.build(ranks.begin(), ranks.size());
        m_dict.build(dict.begin(), dict.size());
    }

    static std::string name() {
        return "sdc";
    }

    uint64_t size() const {
        return m_ranks.size();
    }

    uint64_t num_bits() const {
        return (m_ranks.num_bytes() + m_dict.num_bytes()) * 8;
    }

    uint64_t access(uint64_t i) const {
        uint64_t rank = m_ranks.access(i);
        return m_dict.access(rank);
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visitor.visit(m_ranks);
        visitor.visit(m_dict);
    }

private:
    sdc_sequence m_ranks;
    bits::compact_vector m_dict;
};

template <typename Front, typename Back>
struct dual {
    template <typename Iterator>
    void encode(Iterator begin, uint64_t n) {
        uint64_t front_size = n * 0.3;
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
typedef dual<compact, compact> compact_compact;
typedef dual<dictionary, dictionary> dictionary_dictionary;
typedef dual<dictionary, elias_fano> dictionary_elias_fano;

}  // namespace pthash