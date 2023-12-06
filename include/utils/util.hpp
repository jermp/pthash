#pragma once

#include <chrono>
#include <string>

#include "../../external/essentials/include/essentials.hpp"
#include "../../external/fastmod/fastmod.h"

#define PTHASH_LIKELY(expr) __builtin_expect((bool)(expr), true)

namespace pthash {

typedef std::chrono::high_resolution_clock clock_type;

namespace constants {
static const uint64_t available_ram = sysconf(_SC_PAGESIZE) * sysconf(_SC_PHYS_PAGES);
static const uint64_t invalid_seed = uint64_t(-1);
static const uint64_t invalid_num_buckets = uint64_t(-1);
static const uint64_t min_partition_size = 10000;

static const std::string default_tmp_dirname(".");

/* p1=n*a keys are placed in p2=m*b buckets */
constexpr float a = 0.6;
constexpr float b = 0.3;
/****************************************/

}  // namespace constants

static inline uint64_t random_value() {
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::mt19937_64 rng(seed);
    return rng();
}

template <typename DurationType>
double seconds(DurationType const& d) {
    return static_cast<double>(std::chrono::duration_cast<std::chrono::milliseconds>(d).count()) /
           1000;  // better resolution than std::chrono::seconds
}

}  // namespace pthash