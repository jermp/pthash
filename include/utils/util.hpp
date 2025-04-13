#pragma once

#include <chrono>
#include <string>

#include "essentials.hpp"
#include "fastmod.h"
#include "hasher.hpp"

#define PTHASH_LIKELY(expr) __builtin_expect((bool)(expr), true)

namespace pthash {

typedef std::chrono::high_resolution_clock clock_type;

namespace constants {

static const uint64_t available_ram = sysconf(_SC_PAGESIZE) * sysconf(_SC_PHYS_PAGES);
static const uint64_t invalid_seed = uint64_t(-1);
static const uint64_t invalid_num_buckets = uint64_t(-1);
static const uint64_t invalid_table_size = uint64_t(-1);
static const uint64_t min_partition_size = 1000;
static const uint64_t max_partition_size = 5000;

static const std::string default_tmp_dirname(".");

/* For a skew_becketer: a*n keys are placed in b*m buckets */
constexpr float a = 0.6;
constexpr float b = 0.3;
/***********************************************************/

}  // namespace constants

static inline uint64_t mul_high(const uint64_t x, const uint64_t y) {
    return (uint64_t)(((__uint128_t)x * (__uint128_t)y) >> 64);
}

static inline uint64_t remap128(const uint64_t hash, const uint64_t n) {
    uint64_t ret = mul_high(hash, n);
    assert(ret < n);
    return ret;
}

static inline uint64_t random_value() {
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::mt19937_64 rng(seed);
    return rng();
}

template <typename DurationType>
double to_microseconds(DurationType const& d) {
    return static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(d).count());
}

}  // namespace pthash