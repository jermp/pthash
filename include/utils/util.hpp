#pragma once

#include <chrono>
#include <algorithm>

#include "../../external/essentials/include/essentials.hpp"
#include "../../external/mm_file/include/mm_file/mm_file.hpp"
#include "../../external/fastmod/fastmod.h"

#define PTH_LIKELY(expr) __builtin_expect((bool)(expr), true)

namespace pthash {

typedef std::chrono::high_resolution_clock clock_type;

namespace constants {
static const uint64_t invalid_seed = uint64_t(-1);
static const uint64_t invalid_num_buckets = uint64_t(-1);
static const std::string default_tmp_dirname(".");
}  // namespace constants

uint64_t random_value() {
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::mt19937_64 rng(seed);
    return rng();
}

template <typename DurationType>
double seconds(DurationType const& d) {
    return static_cast<double>(std::chrono::duration_cast<std::chrono::milliseconds>(d).count()) /
           1000;  // better resolution than std::chrono::seconds
}

struct file_lines_iterator {
    typedef std::string value_type;

    file_lines_iterator(char const* filename) : m_is(filename, mm::advice::sequential) {
        m_data = m_is.data();
    }

    std::string operator*() {
        uint8_t const* begin = m_data;
        while (*m_data++ != '\n')
            ;
        return std::string(reinterpret_cast<const char*>(begin), m_data - begin - 1);
    }

    void operator++(int) {}
    void operator++() {}

private:
    mm::file_source<uint8_t> m_is;
    uint8_t const* m_data;
};

}  // namespace pthash