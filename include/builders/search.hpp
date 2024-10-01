#pragma once

#include <math.h>   // for pow, round, log2
#include <sstream>  // for stringbuf
#include <atomic>   // for std::atomic
#include <vector>

#include "external/bits/include/bit_vector.hpp"

#include "include/builders/util.hpp"
#include "include/utils/hasher.hpp"

#include "search_xor.hpp"
#include "search_add.hpp"

namespace pthash {

constexpr uint64_t search_cache_size = 1000;

struct search_logger {
    search_logger(uint64_t num_keys, uint64_t table_size, uint64_t num_buckets)
        : m_num_keys(num_keys)
        , m_table_size(table_size)
        , m_num_buckets(num_buckets)
        , m_step(m_num_buckets > 20 ? m_num_buckets / 20 : 1)
        , m_bucket(0)
        , m_placed_keys(0)
        , m_trials(0)
        , m_total_trials(0)
        , m_expected_trials(0.0)
        , m_total_expected_trials(0.0) {}

    void init() {
        essentials::logger("search starts");
        m_timer.start();
    }

    /* If X_i is the random variable counting the number of trials
     for bucket i, then Pr(X_i <= N - 1) = 1 - (1 - p_i)^N,
     where p_i is the success probability for bucket i.
     By solving 1 - (1 - p_i)^N >= T wrt N and for a given target
     probability T < 1, we obtain N <= log_{1-p_i}(1-T), that is:
     we get a pilot <= N with probability T.
     Of course, the closer T is to 1, the higher N becomes.
     In practice T = 0.65 suffices to have
        N > # trials per bucket, for all buckets.
     */
    double pilot_wp_T(double T, double p) {
        assert(T > 0 and p > 0);
        double x = std::log2(1.0 - T) / std::log2(1.0 - p);
        return round(x);
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
        essentials::logger("search ends");
        std::cout << " == " << m_num_buckets - bucket << " empty buckets ("
                  << ((m_num_buckets - bucket) * 100.0) / m_num_buckets << "%)" << std::endl;
        std::cout << " == total trials = " << m_total_trials << std::endl;
        std::cout << " == total expected trials = " << uint64_t(m_total_expected_trials)
                  << std::endl;
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

    void print(uint64_t bucket) {
        m_timer.stop();
        std::stringbuf buffer;
        std::ostream os(&buffer);
        os << m_step << " buckets done in " << m_timer.elapsed() << " seconds ("
           << (m_placed_keys * 100.0) / m_num_keys << "% of keys, "
           << (bucket * 100.0) / m_num_buckets << "% of buckets, "
           << static_cast<double>(m_trials) / m_step << " trials per bucket, "
           << m_expected_trials / m_step << " expected trials per bucket)";
        essentials::logger(buffer.str());
        m_bucket = bucket;
        m_trials = 0;
        m_expected_trials = 0.0;
        m_timer.reset();
        m_timer.start();
    }
};

template <typename BucketsIterator, typename PilotsBuffer>
void search(const uint64_t num_keys, const uint64_t num_buckets,
            const uint64_t num_non_empty_buckets, const uint64_t seed,
            build_configuration const& config, BucketsIterator& buckets, bit_vector_builder& taken,
            PilotsBuffer& pilots) {
    if (config.num_threads > 1) {
        if (config.num_threads > std::thread::hardware_concurrency()) {
            throw std::invalid_argument("parallel search should use at most " +
                                        std::to_string(std::thread::hardware_concurrency()) +
                                        " threads");
        }
        if (config.search == pthash_search_type::xor_displacement) {
            search_parallel_xor(num_keys, num_buckets, num_non_empty_buckets, seed, config, buckets,
                                taken, pilots);
        } else if (config.search == pthash_search_type::add_displacement) {
            search_parallel_add(num_keys, num_buckets, num_non_empty_buckets, seed, config, buckets,
                                taken, pilots);
        } else {
            assert(false);
        }
    } else {
        if (config.search == pthash_search_type::xor_displacement) {
            search_sequential_xor(num_keys, num_buckets, num_non_empty_buckets, seed, config,
                                  buckets, taken, pilots);
        } else if (config.search == pthash_search_type::add_displacement) {
            search_sequential_add(num_keys, num_buckets, num_non_empty_buckets, seed, config,
                                  buckets, taken, pilots);
        } else {
            assert(false);
        }
    }
}

}  // namespace pthash