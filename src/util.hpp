#pragma once

#include <iostream>
#include <iomanip>
#include <vector>
#include <string>
#include <fstream>
#include <chrono>

#include "../include/utils/util.hpp"
#include "../external/essentials/include/essentials.hpp"

namespace pthash {

std::vector<std::string> read_string_collection(uint64_t n, char const* filename) {
    std::ifstream input(filename);
    if (!input.good()) throw std::runtime_error("error in opening file.");
    std::string s;
    uint64_t max_string_length = 0;
    uint64_t sum_of_lengths = 0;
    std::vector<std::string> strings;
    strings.reserve(n);
    while (std::getline(input, s)) {
        if (s.size() > max_string_length) max_string_length = s.size();
        sum_of_lengths += s.size();
        strings.push_back(s);
        if (strings.size() % 1000000 == 0) {
            std::cout << "read " << strings.size() << " strings" << std::endl;
        }
        if (strings.size() == n) break;
    }
    input.close();
    strings.shrink_to_fit();
    std::cout << "num_strings " << strings.size() << std::endl;
    std::cout << "max_string_length " << max_string_length << std::endl;
    std::cout << "total_length " << sum_of_lengths << std::endl;
    std::cout << "avg_string_length " << std::fixed << std::setprecision(2)
              << static_cast<double>(sum_of_lengths) / strings.size() << std::endl;
    return strings;
}

template <typename Uint>
std::vector<Uint> distinct_keys(uint64_t num_keys, uint64_t seed = constants::invalid_seed) {
    auto gen = std::mt19937_64((seed != constants::invalid_seed) ? seed : std::random_device()());
    std::vector<Uint> keys(num_keys * 1.05);       // allocate a vector slightly larger than needed
    std::generate(keys.begin(), keys.end(), gen);  // fill with random numbers
    std::sort(keys.begin(), keys.end());           // sort the keys
    std::unique(keys.begin(), keys.end());         // remove the duplicates
    uint64_t size = keys.size();
    while (size < num_keys) {  // unlikely
        keys.push_back(keys.back() + 1);
        size++;
    }
    assert(std::adjacent_find(keys.begin(), keys.end()) == keys.end());  // must be all distinct
    std::shuffle(keys.begin(), keys.end(), gen);
    if (keys.size() > num_keys) keys.resize(num_keys);
    assert(keys.size() == num_keys);
    return keys;
}

template <typename MPHF, typename Iterator>
bool check(Iterator keys, uint64_t num_keys, MPHF const& f) {
    __uint128_t n = num_keys;
    __uint128_t sum = 0;
    for (uint64_t i = 0; i != n; ++i) {
        auto const& key = *keys;
        uint64_t p = f(key);
        if (p >= n) {
            std::cout << "ERROR: position is out of range" << std::endl;
            return false;
        }
        sum += p;
        ++keys;
    }
    if (sum != (n * (n - 1)) / 2) {
        std::cout << "ERROR: mphf contains duplicates" << std::endl;
        return false;
    }
    return true;
}

template <typename MPHF, typename Iterator>
double perf(Iterator keys, uint64_t num_keys, MPHF const& f) {
    static const uint64_t runs = 5;
    essentials::timer<std::chrono::high_resolution_clock, std::chrono::nanoseconds> t;
    t.start();
    for (uint64_t r = 0; r != runs; ++r) {
        Iterator begin = keys;
        for (uint64_t i = 0; i != num_keys; ++i) {
            auto const& key = *begin;
            uint64_t p = f(key);
            essentials::do_not_optimize_away(p);
            ++begin;
        }
    }
    t.stop();
    double nanosec_per_key = t.elapsed() / static_cast<double>(runs * num_keys);
    return nanosec_per_key;
}

}  // namespace pthash