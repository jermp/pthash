#pragma once

#include <sstream>

#include "util.hpp"
#include "external/spline/src/spline.h"

namespace pthash {

struct opt2_bucketer {
    static constexpr uint64_t FULCS_INTER = 1024;

    /* util */
    struct csv_buckter {
        csv_buckter(std::string const& path) {
            std::ifstream file(path);
            if (!file.is_open()) throw std::runtime_error("error in opening the file");

            std::vector<double> x;
            std::vector<double> y;

            std::string line;
            while (std::getline(file, line)) {
                std::stringstream ss(line);
                std::string cell;
                // Read the first column
                if (std::getline(ss, cell, ',')) {
                    x.push_back(std::stod(cell));
                } else {
                    std::cerr << "Error reading the first column." << std::endl;
                }
                // Read the second column
                if (std::getline(ss, cell, ',')) {
                    y.push_back(std::stod(cell));
                } else {
                    std::cerr << "Error reading the second column." << std::endl;
                }
            }
            m_spline = tk::spline(x, y);
        }

        double spline(const double x) const {
            if (x > 0.9999) return 1.0;
            if (x < 0.0001) return 0.0;
            return m_spline(x);
        }

    private:
        tk::spline m_spline;
    };

    void init(const uint64_t num_buckets) {
        m_num_buckets = num_buckets;
        csv_buckter bucketer("../bucket_mappings/optimizedBucketMapping.csv");
        m_fulcrums.reserve(FULCS_INTER);
        for (uint64_t xi = 0; xi != FULCS_INTER; ++xi) {
            double x = double(xi) / double(FULCS_INTER - 1);
            double y = bucketer.spline(x);
            uint32_t fulcV = uint32_t(y * double(m_num_buckets) * double(1 << 16));
            m_fulcrums.push_back(fulcV);
        }
    }

    inline uint64_t bucket(const uint64_t hash) const {
        uint64_t z = (hash >> 32) * uint64_t(FULCS_INTER - 1);
        uint64_t index = z >> 32;
        assert(index + 1 < m_fulcrums.size());
        uint64_t part = z & 0xFFFFFFFF;
        uint64_t v1 = (m_fulcrums[index + 0] * part) >> 32;
        uint64_t v2 = (m_fulcrums[index + 1] * (0xFFFFFFFF - part)) >> 32;
        uint64_t bucket_id = (v1 + v2) >> 16;
        assert(bucket_id < num_buckets());
        return bucket_id;
    }

    uint64_t num_buckets() const {
        return m_num_buckets;
    }

    size_t num_bits() const {
        return 8 * (sizeof(m_num_buckets) + essentials::vec_bytes(m_fulcrums));
    }

    template <typename Visitor>
    void visit(Visitor& visitor) {
        visitor.visit(m_num_buckets);
        visitor.visit(m_fulcrums);
    }

private:
    uint64_t m_num_buckets;
    std::vector<uint32_t> m_fulcrums;
};

struct opt1_bucketer {
    opt1_bucketer() {}

    void init(const uint64_t num_buckets) {
        m_num_buckets = num_buckets;
    }

    inline uint64_t bucket(const uint64_t hash) const {
        double normalized_hash = double(hash) / double(~0ul);
        constexpr double c = 0.08;
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
    static constexpr float a = 0.6;  // p1=n*a keys are placed in
    static constexpr float b = 0.3;  // p2=m*b buckets

    skew_bucketer() {}

    void init(const uint64_t num_buckets) {
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

struct uniform_bucketer {
    uniform_bucketer() {}

    void init(const uint64_t num_buckets) {
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