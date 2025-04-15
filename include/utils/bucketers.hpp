#pragma once

#include "utils/util.hpp"

namespace pthash {

struct opt_bucketer {
    opt_bucketer() : m_num_buckets(0) {}

    void init(const uint64_t num_buckets) {
        m_num_buckets = num_buckets;
    }

    inline uint64_t bucket(uint64_t hash) const {
        /*
            This
                x * x * (1 + x)/2 * 255/256 + x/256
            is a fast approximation of the optimal bucketing function
            introduced by PHOBIC. The approximation is described in
            "PtrHash: Minimal Perfect Hashing at RAM Throughput",
            https://arxiv.org/abs/2502.15539. Ragnar Groot Koerkamp. 2025.
        */
        uint64_t H =
            mul_high(mul_high(hash, hash), (hash >> 1) | (1ULL << 63)) / 256 * 255 + hash / 256;
        return remap128(H, m_num_buckets);
    }

    uint64_t num_buckets() const {
        return m_num_buckets;
    }

    uint64_t num_bits() const {
        return 8 * sizeof(m_num_buckets);
    }

    void swap(opt_bucketer& other) {
        std::swap(m_num_buckets, other.m_num_buckets);
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
        visitor.visit(t.m_num_buckets);
    }

    uint64_t m_num_buckets;
};

struct skew_bucketer {
    skew_bucketer() : m_num_dense_buckets(0), m_num_sparse_buckets(0) {}

    void init(const uint64_t num_buckets) {
        m_num_dense_buckets = constants::b * num_buckets;
        m_num_sparse_buckets = num_buckets - m_num_dense_buckets;
    }

    inline uint64_t bucket(uint64_t hash) const {
        static const uint64_t T = constants::a * static_cast<double>(UINT64_MAX);
        uint64_t H = hash << 32;
        return (hash < T) ? remap128(H, m_num_dense_buckets)
                          : m_num_dense_buckets + remap128(H, m_num_sparse_buckets);
    }

    uint64_t num_buckets() const {
        return m_num_dense_buckets + m_num_sparse_buckets;
    }

    uint64_t num_bits() const {
        return 8 * (sizeof(m_num_dense_buckets) + sizeof(m_num_sparse_buckets));
    }

    void swap(skew_bucketer& other) {
        std::swap(m_num_dense_buckets, other.m_num_dense_buckets);
        std::swap(m_num_sparse_buckets, other.m_num_sparse_buckets);
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
        visitor.visit(t.m_num_dense_buckets);
        visitor.visit(t.m_num_sparse_buckets);
    }

    uint64_t m_num_dense_buckets, m_num_sparse_buckets;
};

struct range_bucketer {
    range_bucketer() : m_num_buckets(0) {}

    void init(const uint64_t num_buckets) {
        if (num_buckets > (1ULL << 32)) throw std::runtime_error("too many buckets");
        m_num_buckets = num_buckets;
    }

    inline uint64_t bucket(const uint64_t hash) const {
        return ((hash >> 32) * m_num_buckets) >> 32;
    }

    uint64_t num_buckets() const {
        return m_num_buckets;
    }

    uint64_t num_bits() const {
        return 8 * sizeof(m_num_buckets);
    }

    void swap(range_bucketer& other) {
        std::swap(m_num_buckets, other.m_num_buckets);
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
        visitor.visit(t.m_num_buckets);
    }

    uint64_t m_num_buckets;
};

}  // namespace pthash