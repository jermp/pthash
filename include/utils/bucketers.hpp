#pragma once

#include <array>

#include "utils/util.hpp"

namespace pthash {

template <typename Bucketer>
struct table_bucketer {
    table_bucketer() : m_base(Bucketer()), m_fulcrums() {}

    void init(const uint64_t num_buckets, const double lambda, const uint64_t table_size,
              const double alpha) {
        m_base.init(num_buckets, lambda, table_size, alpha);

        m_fulcrums[0] = 0;
        for (size_t xi = 0; xi < FULCS - 1; xi++) {
            double x = double(xi) / double(FULCS - 1);
            double y = m_base.bucketRelative(x);
            auto fulcV = uint64_t(y * double(num_buckets << 16));
            m_fulcrums[xi + 1] = fulcV;
        }
        m_fulcrums[FULCS - 1] = num_buckets << 16;
    }

    inline uint64_t bucket(const uint64_t hash) const {
        uint64_t z = (hash & 0xFFFFFFFF) * uint64_t(FULCS - 1);
        uint64_t index = z >> 32;
        uint64_t part = z & 0xFFFFFFFF;
        uint64_t v1 = (m_fulcrums[index + 0] * part) >> 32;
        uint64_t v2 = (m_fulcrums[index + 1] * (0xFFFFFFFF - part)) >> 32;
        return (v1 + v2) >> 16;
    }

    uint64_t num_buckets() const {
        return m_base.num_buckets();
    }

    size_t num_bits() const {
        return m_base.num_buckets() + m_fulcrums.size() * 64;
    }

    void swap(table_bucketer& other) {
        m_base.swap(other.m_base);
        std::swap(m_fulcrums, other.m_fulcrums);
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
        visitor.visit(t.m_fulcrums);
        visitor.visit(t.m_base);
    }

    Bucketer m_base;
    static const uint64_t FULCS = 2048;
    std::array<uint64_t, FULCS> m_fulcrums;
};

struct opt_bucketer {
    opt_bucketer() : m_c(0), m_num_buckets(0), m_alpha(0), m_alpha_factor(0) {}

    inline double baseFunc(const double normalized_hash) const {
        return (normalized_hash + (1 - normalized_hash) * std::log(1 - normalized_hash)) *
                   (1.0 - m_c) +
               m_c * normalized_hash;
    }

    void init(const uint64_t num_buckets, const double lambda, const uint64_t table_size,
              const double alpha) {
        m_num_buckets = num_buckets;
        m_alpha = alpha;
        m_c = 0.2 * lambda / std::sqrt(table_size);
        if (alpha > 0.9999) {
            m_alpha_factor = 1.0;
        } else {
            m_alpha_factor = 1.0 / baseFunc(alpha);
        }
    }

    inline double bucketRelative(const double normalized_hash) const {
        return m_alpha_factor * baseFunc(m_alpha * normalized_hash);
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
        return 8 * sizeof(m_num_buckets) + 8 * sizeof(m_c) + 8 * sizeof(m_alpha) +
               8 * sizeof(m_alpha_factor);
    }

    void swap(opt_bucketer& other) {
        std::swap(m_c, other.m_c);
        std::swap(m_num_buckets, other.m_num_buckets);
        std::swap(m_alpha, other.m_alpha);
        std::swap(m_alpha_factor, other.m_alpha_factor);
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
        visitor.visit(t.m_c);
        visitor.visit(t.m_alpha);
        visitor.visit(t.m_alpha_factor);
    }

    double m_c;
    uint64_t m_num_buckets;
    double m_alpha;
    double m_alpha_factor;
};

struct skew_bucketer {
    skew_bucketer()
        : m_num_dense_buckets(0)
        , m_num_sparse_buckets(0)
        , m_M_num_dense_buckets(0)
        , m_M_num_sparse_buckets(0) {}

    void init(const uint64_t num_buckets, const double /* lambda */,
              const uint64_t /* table_size */, const double /* alpha */) {
        m_num_dense_buckets = constants::b * num_buckets;
        m_num_sparse_buckets = num_buckets - m_num_dense_buckets;
        m_M_num_dense_buckets =
            m_num_dense_buckets > 0 ? fastmod::computeM_u64(m_num_dense_buckets) : 0;
        m_M_num_sparse_buckets =
            m_num_sparse_buckets > 0 ? fastmod::computeM_u64(m_num_sparse_buckets) : 0;
    }

    inline uint64_t bucket(uint64_t hash) const {
        static const uint64_t T = constants::a * static_cast<double>(UINT64_MAX);
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
        visitor.visit(t.m_M_num_dense_buckets);
        visitor.visit(t.m_M_num_sparse_buckets);
    }

    uint64_t m_num_dense_buckets, m_num_sparse_buckets;
    __uint128_t m_M_num_dense_buckets, m_M_num_sparse_buckets;
};

struct range_bucketer {
    range_bucketer() : m_num_buckets(0), m_M_num_buckets(0) {}

    void init(const uint64_t num_buckets) {
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

    void swap(range_bucketer& other) {
        std::swap(m_num_buckets, other.m_num_buckets);
        std::swap(m_M_num_buckets, other.m_M_num_buckets);
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
        visitor.visit(t.m_M_num_buckets);
    }

    uint64_t m_num_buckets;
    __uint128_t m_M_num_buckets;
};

struct uniform_bucketer {
    uniform_bucketer() : m_num_buckets(0), m_M_num_buckets(0) {}

    void init(const uint64_t num_buckets, const double /* lambda */,
              const uint64_t /* table_size */, const double /* alpha */) {
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

    void swap(uniform_bucketer& other) {
        std::swap(m_num_buckets, other.m_num_buckets);
        std::swap(m_M_num_buckets, other.m_M_num_buckets);
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
        visitor.visit(t.m_M_num_buckets);
    }

    uint64_t m_num_buckets;
    __uint128_t m_M_num_buckets;
};

}  // namespace pthash