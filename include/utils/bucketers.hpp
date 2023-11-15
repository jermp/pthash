#pragma once

#include "util.hpp"

namespace pthash {

struct opt_bucketer {
    opt_bucketer() {}

    void init(uint64_t num_buckets) {
        m_num_buckets = num_buckets;
    }

    inline uint64_t bucket(uint64_t hash) const {
        double normalized_hash = static_cast<double>(hash) / double(~0ul);
        constexpr double c = 0.080;
        double normalized_bucket =
            (normalized_hash + (1 - normalized_hash) * std::log(1 - normalized_hash)) * (1 - c) +
            normalized_hash * c;
        uint64_t bucket_id =
            std::min(uint64_t(normalized_bucket * m_num_buckets), m_num_buckets - 1);
        assert(bucket_id < num_buckets());
        return bucket_id;
    }

    uint64_t num_buckets() const {
        return m_num_buckets;
    }

    size_t num_bits() const {
        return 8 * sizeof(m_num_buckets);
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visitor.visit(m_num_buckets);
    }

private:
    uint64_t m_num_buckets;
};

struct skew_bucketer {
    skew_bucketer() {}

    void init(uint64_t num_buckets) {
        m_num_dense_buckets = constants::b * num_buckets;
        m_num_sparse_buckets = num_buckets - m_num_dense_buckets;
        m_M_num_dense_buckets = fastmod::computeM_u64(m_num_dense_buckets);
        m_M_num_sparse_buckets = fastmod::computeM_u64(m_num_sparse_buckets);
    }

    inline uint64_t bucket(uint64_t hash) const {
        static const uint64_t T = constants::a * UINT64_MAX;
        return (hash < T) ? fastmod::fastmod_u64(hash, m_M_num_dense_buckets, m_num_dense_buckets)
                          : m_num_dense_buckets + fastmod::fastmod_u64(hash, m_M_num_sparse_buckets,
                                                                       m_num_sparse_buckets);
    }

    uint64_t num_buckets() const {
        return m_num_dense_buckets + m_num_sparse_buckets;
    }

    size_t num_bits() const {
        return 8 * (sizeof(m_num_dense_buckets) + sizeof(m_num_sparse_buckets) +
                    sizeof(m_M_num_dense_buckets) + sizeof(m_M_num_sparse_buckets));
    }

    void swap(skew_bucketer& other) {
        std::swap(m_num_dense_buckets, other.m_num_dense_buckets);
        std::swap(m_num_sparse_buckets, other.m_num_sparse_buckets);
        std::swap(m_M_num_dense_buckets, other.m_M_num_dense_buckets);
        std::swap(m_M_num_sparse_buckets, other.m_M_num_sparse_buckets);
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visitor.visit(m_num_dense_buckets);
        visitor.visit(m_num_sparse_buckets);
        visitor.visit(m_M_num_dense_buckets);
        visitor.visit(m_M_num_sparse_buckets);
    }

private:
    uint64_t m_num_dense_buckets, m_num_sparse_buckets;
    __uint128_t m_M_num_dense_buckets, m_M_num_sparse_buckets;
};

struct uniform_bucketer {
    uniform_bucketer() {}

    void init(uint64_t num_buckets) {
        m_num_buckets = num_buckets;
        m_M_num_buckets = fastmod::computeM_u64(m_num_buckets);
    }

    inline uint64_t bucket(uint64_t hash) const {
        return fastmod::fastmod_u64(hash, m_M_num_buckets, m_num_buckets);
    }

    uint64_t num_buckets() const {
        return m_num_buckets;
    }

    size_t num_bits() const {
        return 8 * (sizeof(m_num_buckets) + sizeof(m_M_num_buckets));
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visitor.visit(m_num_buckets);
        visitor.visit(m_M_num_buckets);
    }

private:
    uint64_t m_num_buckets;
    __uint128_t m_M_num_buckets;
};

}  // namespace pthash