#pragma once

#include "../../external/essentials/include/essentials.hpp"

#include "compact_vector.hpp"
#include "ef_sequence.hpp"
#include "sdc_sequence.hpp"

#include <vector>
#include <unordered_map>
#include <cassert>

namespace pthash {

struct compact {
    template <typename T>
    void encode(std::vector<T> const& values) {
        m_values.build(values.begin(), values.size());
    }

    static std::string name() {
        return "compact";
    }

    size_t size() const {
        return m_values.size();
    }

    size_t num_bits() const {
        return m_values.bytes() * 8;
    }

    uint64_t access(uint64_t i) const {
        return m_values.access(i);
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visitor.visit(m_values);
    }

private:
    compact_vector m_values;
};

struct partitioned_compact {
    static const uint64_t partition_size = 256;
    static_assert(partition_size > 0);

    template <typename T>
    void encode(std::vector<T> const& values) {
        uint64_t num_values = values.size();
        uint64_t num_partitions = (num_values + partition_size - 1) / partition_size;
        bit_vector_builder bvb;
        bvb.reserve(32 * num_values);
        m_bits_per_value.reserve(num_partitions + 1);
        m_bits_per_value.push_back(0);
        for (uint64_t i = 0, begin = 0; i != num_partitions; ++i) {
            uint64_t end = begin + partition_size;
            if (end > num_values) end = num_values;
            uint64_t max_value = 0;
            for (uint64_t k = begin; k != end; ++k) {
                if (values[k] > max_value) max_value = values[k];
            }
            uint64_t num_bits = (max_value == 0) ? 1 : std::ceil(std::log2(max_value + 1));
            assert(num_bits > 0);

            // std::cout << i << ": " << num_bits << '\n';
            // for (uint64_t k = begin; k != end; ++k) {
            //     uint64_t num_bits_val = (values[k] == 0) ? 1 : std::ceil(std::log2(values[k] +
            //     1)); std::cout << "  " << num_bits_val << '/' << num_bits << '\n';
            // }

            for (uint64_t k = begin; k != end; ++k) bvb.append_bits(values[k], num_bits);
            assert(m_bits_per_value.back() + num_bits < (1ULL << 32));
            m_bits_per_value.push_back(m_bits_per_value.back() + num_bits);
            begin = end;
        }
        m_values.build(&bvb);
    }

    static std::string name() {
        return "partitioned_compact";
    }

    size_t size() const {
        return m_size;
    }

    size_t num_bits() const {
        return (sizeof(m_size) + m_bits_per_value.size() * sizeof(m_bits_per_value.front()) +
                m_values.bytes()) *
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
    void visit(Visitor& visitor) {
        visitor.visit(m_size);
        visitor.visit(m_bits_per_value);
        visitor.visit(m_values);
    }

private:
    uint64_t m_size;
    std::vector<uint32_t> m_bits_per_value;
    bit_vector m_values;
};

std::pair<std::vector<uint64_t>, std::vector<uint64_t>> compute_ranks_and_dictionary(
    std::vector<uint64_t> const& values) {
    // accumulate frequencies
    std::unordered_map<uint64_t, uint64_t> distinct;
    for (auto v : values) {
        auto it = distinct.find(v);
        if (it != distinct.end()) {  // found
            (*it).second += 1;
        } else {
            distinct[v] = 1;
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
    ranks.reserve(values.size());
    for (auto v : values) ranks.push_back(distinct[v]);
    return {ranks, dict};
}

struct dictionary {
    void encode(std::vector<uint64_t> const& values) {
        auto [ranks, dict] = compute_ranks_and_dictionary(values);
        m_ranks.build(ranks.begin(), ranks.size());
        m_dict.build(dict.begin(), dict.size());
    }

    static std::string name() {
        return "dictionary";
    }

    size_t size() const {
        return m_ranks.size();
    }

    size_t num_bits() const {
        return (m_ranks.bytes() + m_dict.bytes()) * 8;
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
    compact_vector m_ranks;
    compact_vector m_dict;
};

struct elias_fano {
    template <typename T>
    void encode(std::vector<T> const& values) {
        // take prefix sums and encode
        std::vector<uint64_t> tmp;
        tmp.reserve(1 + values.size());
        tmp.push_back(0);
        for (auto val : values) tmp.push_back(tmp.back() + val);
        m_values.encode(tmp);
    }

    static std::string name() {
        return "elias_fano";
    }

    size_t size() const {
        return m_values.size();
    }

    size_t num_bits() const {
        return m_values.num_bits();
    }

    uint64_t access(uint64_t i) const {
        assert(i + 1 < m_values.size());
        return m_values.diff(i);
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visitor.visit(m_values);
    }

private:
    ef_sequence m_values;
};

struct sdc {
    void encode(std::vector<uint64_t> const& values) {
        auto [ranks, dict] = compute_ranks_and_dictionary(values);
        m_ranks.build(ranks.begin(), ranks.size());
        m_dict.build(dict.begin(), dict.size());
    }

    static std::string name() {
        return "sdc";
    }

    size_t size() const {
        return m_ranks.size();
    }

    size_t num_bits() const {
        return (m_ranks.bytes() + m_dict.bytes()) * 8;
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
    compact_vector m_dict;
};

template <typename Front, typename Back>
struct dual {
    void encode(std::vector<uint64_t> const& values) {
        std::vector<uint64_t> front;
        std::vector<uint64_t> back;
        size_t front_size = values.size() * 0.3;
        front.reserve(front_size);
        back.reserve(values.size() - front_size);
        for (uint64_t i = 0; i != front_size; ++i) front.push_back(values[i]);
        for (uint64_t i = front_size; i != values.size(); ++i) back.push_back(values[i]);
        m_front.encode(front);
        m_back.encode(back);
    }

    static std::string name() {
        return Front::name() + "-" + Back::name();
    }

    size_t num_bits() const {
        return m_front.num_bits() + m_back.num_bits();
    }

    uint64_t access(uint64_t i) const {
        if (i < m_front.size()) return m_front.access(i);
        return m_back.access(i - m_front.size());
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visitor.visit(m_front);
        visitor.visit(m_back);
    }

private:
    Front m_front;
    Back m_back;
};

/* dual encoders */
typedef dual<compact, compact> compact_compact;
typedef dual<dictionary, dictionary> dictionary_dictionary;
typedef dual<dictionary, elias_fano> dictionary_elias_fano;

}  // namespace pthash