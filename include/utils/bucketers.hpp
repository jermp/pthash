#pragma once

#include <sstream>

#include "util.hpp"

namespace pthash {

template<typename Bucketer>
struct table_bucketer {
    table_bucketer() : base(Bucketer()) {}


    void init(const uint64_t num_buckets,                      //
              const double lambda, const uint64_t table_size)  //
    {
        base.init(num_buckets, lambda, table_size);

        for (size_t xi = 0; xi < FULCS; xi++) {
            double x = double(xi) / double(FULCS);
            double y = base.bucketRelative(x);
            uint64_t fulcV = uint64_t(y * double(num_buckets) * double(1 << 16));
            fulcrums.push_back(fulcV);
        }
    }

    inline uint64_t bucket(const uint64_t hash) const {
        uint64_t z = (hash & 0xFFFFFFFF) * uint64_t(FULCS - 1);
        uint64_t index = z >> 32;
        uint64_t part = z & 0xFFFFFFFF;
        uint64_t v1 = (fulcrums[index + 0] * part) >> 32;
        uint64_t v2 = (fulcrums[index + 1] * (0xFFFFFFFF - part)) >> 32;
        return (v1 + v2) >> 16;
    }

    uint64_t num_buckets() const {
        return base.num_buckets();
    }

    size_t num_bits() const {
        return base.num_buckets() + fulcrums.size() * 64;
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visitor.visit(fulcrums);
        visitor.visit(base);
    }

private:
    Bucketer base;
    static const uint64_t FULCS = 2048;
    std::vector<uint64_t> fulcrums;
};

struct opt_bucketer {
    opt_bucketer() {}

    void init(const uint64_t num_buckets,                      //
              const double lambda, const uint64_t table_size)  //
    {
        constexpr double local_collision_factor = 0.5;
        m_num_buckets = num_buckets;
        slope = std::max(
            0.05, std::min(1.0, local_collision_factor * lambda / std::sqrt((double)table_size)));
    }

    inline double bucketRelative(const double normalized_hash) const {
        return std::max(normalized_hash + (1 - normalized_hash) * std::log(1 - normalized_hash),
                            slope * normalized_hash);;
    }

    inline uint64_t bucket(const uint64_t hash) const {
        double normalized_hash = double(hash) / double(~0ul);
        double normalized_bucket = bucketRelative(normalized_hash);
        uint64_t bucket_id =
            std::min(uint64_t(normalized_bucket * m_num_buckets), m_num_buckets - 1);
        assert(bucket_id < num_buckets());
        return bucket_id;
    }

    uint64_t num_buckets() const {
        return m_num_buckets;
    }

    size_t num_bits() const {
        return 8 * sizeof(m_num_buckets) + 8 * sizeof(slope);
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visitor.visit(m_num_buckets);
        visitor.visit(slope);
    }

private:
    double slope;
    uint64_t m_num_buckets;
};

struct skew_bucketer {
    static constexpr float a = 0.6;  // p1=n*a keys are placed in
    static constexpr float b = 0.3;  // p2=m*b buckets

    skew_bucketer() {}

    void init(const uint64_t num_buckets,               //
              const double /* lambda */, const uint64_t /* table_size */
    ) {
        m_num_dense_buckets = b * num_buckets;
        m_num_sparse_buckets = num_buckets - m_num_dense_buckets;
        m_M_num_dense_buckets = fastmod::computeM_u64(m_num_dense_buckets);
        m_M_num_sparse_buckets = fastmod::computeM_u64(m_num_sparse_buckets);
    }

    inline uint64_t bucket(const uint64_t hash) const {
        static const uint64_t T = a * UINT64_MAX;
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

struct range_bucketer {
    range_bucketer() {}

    void init(const uint64_t num_buckets)  //
    {
        m_num_buckets = num_buckets;
    }

    inline uint64_t bucket(const uint64_t hash) const {
        return ((hash >> 32U) * m_num_buckets) >> 32U;
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

struct uniform_bucketer {
    uniform_bucketer() {}

    void init(const uint64_t num_buckets,                                  //
              const double /* lambda */, const uint64_t /* table_size */)  //
    {
        m_num_buckets = num_buckets;
        m_M_num_buckets = fastmod::computeM_u64(m_num_buckets);
    }

    inline uint64_t bucket(const uint64_t hash) const {
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