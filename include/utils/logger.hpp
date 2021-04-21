#pragma once

#include <math.h>  // for pow

#include "../../external/essentials/include/essentials.hpp"

namespace pthash {

struct logger {
    logger(uint64_t num_keys, uint64_t table_size, uint64_t num_buckets)
        : m_num_keys(num_keys)
        , m_table_size(table_size)
        , m_num_buckets(num_buckets)
        , m_step(m_num_buckets > 20 ? m_num_buckets / 20 : 1)
        , m_bucket(0)
        , m_placed_keys(0)
        , m_trials(0)
        , m_total_trials(0)
        , m_expected_trials(0.0)
        , m_total_expected_trials(0.0) {
        m_timer.start();
    }

    void update(uint64_t bucket, uint64_t bucket_size, uint64_t pilot) {
        if (bucket > 0) {
            double base = static_cast<double>(m_table_size - m_placed_keys) / m_table_size;
            double p = pow(base, bucket_size);
            double e = 1.0 / p;
            m_expected_trials += e;
            m_total_expected_trials += e;
        }

        m_placed_keys += bucket_size;
        m_trials += pilot + 1;
        m_total_trials += pilot + 1;

        if (bucket > 0 and bucket % m_step == 0) print(bucket);
    }

    void finalize(uint64_t bucket) {
        m_step = bucket - m_bucket;
        print(bucket);
        std::cout << " == " << m_num_buckets - bucket << " empty buckets ("
                  << ((m_num_buckets - bucket) * 100.0) / m_num_buckets << "%)" << std::endl;
        std::cout << " == total trials = " << m_total_trials << std::endl;
        std::cout << " == total expected trials = " << uint64_t(m_total_expected_trials)
                  << std::endl;
    }

    void print(uint64_t bucket) {
        m_timer.stop();
        std::cout << "  == " << m_step << " buckets done in " << m_timer.elapsed() << " seconds ("
                  << (m_placed_keys * 100.0) / m_num_keys << "% of keys, "
                  << (bucket * 100.0) / m_num_buckets << "% of buckets, "
                  << static_cast<double>(m_trials) / m_step << " trials per bucket, "
                  << m_expected_trials / m_step << " expected trials per bucket)\n";
        m_bucket = bucket;
        m_trials = 0;
        m_expected_trials = 0.0;
        m_timer.reset();
        m_timer.start();
    }

private:
    uint64_t m_num_keys;
    uint64_t m_table_size;
    uint64_t m_num_buckets;
    uint64_t m_step;
    uint64_t m_bucket;
    uint64_t m_placed_keys;

    uint64_t m_trials;
    uint64_t m_total_trials;
    double m_expected_trials;
    double m_total_expected_trials;

    essentials::timer<std::chrono::high_resolution_clock, std::chrono::seconds> m_timer;
};

}  // namespace pthash