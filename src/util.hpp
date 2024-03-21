#pragma once

#include <chrono>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <iterator>
#include <sstream>  // for stringbuf
#include <string>
#include <vector>

#include "include/utils/util.hpp"
#include "essentials.hpp"

namespace pthash {

struct lines_iterator : std::forward_iterator_tag {
    typedef std::string value_type;

    lines_iterator(uint8_t const* begin, uint8_t const* end)
        : m_begin(begin), m_end(end), m_num_lines(0), m_num_empty_lines(0) {}

    std::string operator*() {
        uint8_t const* begin = m_begin;
        while (m_begin != m_end and *m_begin++ != '\n')
            ;

        if (m_begin <= begin + 1) {
            std::stringbuf buffer;
            std::ostream os(&buffer);
            if (m_begin == m_end) {
                os << "reached end of file after " << m_num_lines << " lines";
            } else {
                os << "second blank line detected after " << m_num_lines << " lines";
                ++m_num_empty_lines;
            }
            /* does not allow more than 1 empty key */
            if (m_num_empty_lines > 1 or m_begin == m_end) throw std::runtime_error(buffer.str());
        }

        ++m_num_lines;
        return std::string(reinterpret_cast<const char*>(begin), m_begin - begin - 1);
    }

    void operator++(int) const {}
    void operator++() const {}
    lines_iterator operator+(uint64_t) const {
        throw std::runtime_error("lines_iterator::operator+(uint64_t) has not been implemented");
    }

private:
    uint8_t const* m_begin;
    uint8_t const* m_end;
    uint64_t m_num_lines;
    uint64_t m_num_empty_lines;
};

struct sequential_lines_iterator : std::forward_iterator_tag {
    typedef std::string value_type;

    sequential_lines_iterator(std::istream& is)
        : m_pis(&is), m_num_lines(0), m_num_empty_lines(0) {}

    std::string operator*()  //
    {
        std::getline(*m_pis, m_key);

        if (!m_pis->good() || m_key.empty()) {
            std::stringbuf buffer;
            std::ostream os(&buffer);
            if (!m_pis->good()) {
                os << "reached end of file after " << m_num_lines << " lines";
            } else {
                os << "second blank line detected after " << m_num_lines << " lines";
                ++m_num_empty_lines;
            }
            /* does not allow more than 1 empty key */
            if (m_num_empty_lines > 1 or !m_pis->good()) throw std::runtime_error(buffer.str());
        }

        ++m_num_lines;
        return m_key;
    }

    void operator++(int) const {}
    void operator++() const {}
    sequential_lines_iterator operator+(uint64_t) const {
        throw std::runtime_error(
            "sequential_lines_iterator::operator+(uint64_t) has not been implemented");
    }

private:
    std::istream* m_pis;
    uint64_t m_num_lines;
    uint64_t m_num_empty_lines;
    std::string m_key;
};

template <typename IStream>
std::vector<std::string> read_string_collection(uint64_t n, IStream& is, bool verbose) {
    progress_logger logger(n, "read ", " keys from file", verbose);
    std::string s;
    uint64_t max_string_length = 0;
    uint64_t sum_of_lengths = 0;
    std::vector<std::string> strings;
    strings.reserve(n);
    while (std::getline(is, s)) {
        if (s.size() > max_string_length) max_string_length = s.size();
        sum_of_lengths += s.size();
        strings.push_back(s);
        logger.log();
        if (strings.size() == n) break;
    }
    strings.shrink_to_fit();
    logger.finalize();
    if (verbose) {
        std::cout << "num_strings " << strings.size() << std::endl;
        std::cout << "max_string_length " << max_string_length << std::endl;
        std::cout << "total_length " << sum_of_lengths << std::endl;
        std::cout << "avg_string_length " << std::fixed << std::setprecision(2)
                  << static_cast<double>(sum_of_lengths) / strings.size() << std::endl;
    }
    return strings;
}

template <typename Uint>
std::vector<Uint> distinct_keys(uint64_t num_keys, uint64_t seed = constants::invalid_seed) {
    assert(num_keys > 0);
    auto gen = std::mt19937_64((seed != constants::invalid_seed) ? seed : std::random_device()());
    std::vector<Uint> keys(num_keys * 1.05);       // allocate a vector slightly larger than needed
    std::generate(keys.begin(), keys.end(), gen);  // fill with random numbers
    std::sort(keys.begin(), keys.end());
    auto it = std::unique(keys.begin(), keys.end());
    uint64_t size = std::distance(keys.begin(), it);
    if (size < num_keys) {  // unlikely
        uint64_t end = size;
        assert(end > 0);
        for (uint64_t i = 0; i != end - 1 and size != num_keys; ++i) {
            uint64_t first = keys[i];
            uint64_t second = keys[i + 1];
            /* fill the gaps */
            for (uint64_t val = first + 1; val != second; ++val) {
                keys[size] = val;
                ++size;
                if (size == num_keys) break;
            }
        }
    }
    keys.resize(num_keys);
    std::shuffle(keys.begin(), keys.end(), gen);
    return keys;
}

template <typename Function, typename Iterator>
bool check(Iterator keys, Function const& f) {
    __uint128_t n = f.num_keys();
    if (Function::minimal) {
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
    } else {
        uint64_t m = f.table_size();
        bit_vector_builder taken(m);
        for (uint64_t i = 0; i != n; ++i) {
            auto const& key = *keys;
            uint64_t p = f(key);
            if (p >= m) {
                std::cout << "ERROR: position is out of range" << std::endl;
                return false;
            }
            if (taken.get(p) != 0) {
                std::cout << "ERROR: mphf contains duplicates" << std::endl;
                return false;
            }
            taken.set(p, 1);
            ++keys;
        }
    }
    return true;
}

template <typename Function, typename Iterator>
double perf(Iterator keys, uint64_t num_keys, Function const& f) {
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