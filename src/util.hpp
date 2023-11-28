#pragma once

#include <chrono>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <iterator>
#include <sstream>  // for stringbuf
#include <string>
#include <vector>

#include "../include/utils/util.hpp"
#include "../external/essentials/include/essentials.hpp"

namespace pthash {

struct lines_iterator : std::forward_iterator_tag {
    typedef std::string value_type;

    lines_iterator(uint8_t const* begin, uint8_t const* end)
        : m_begin(begin), m_end(end), m_num_nonempty_lines(0), m_num_empty_lines(0) {}

    std::string operator*() {
        uint8_t const* begin = m_begin;
        while (m_begin != m_end and *m_begin++ != '\n')
            ;

        if (m_begin <= begin + 1) {
            std::stringbuf buffer;
            std::ostream os(&buffer);
            if (m_begin == m_end) {
                os << "reached end of file";
            } else {
                os << "blank line detected";
                ++m_num_empty_lines;
            }
            os << " after reading " << m_num_nonempty_lines << " non-empty lines";
            /* does not allow more than 1 empty key */
            if (m_num_empty_lines > 1 or m_begin == m_end) throw std::runtime_error(buffer.str());
        }

        ++m_num_nonempty_lines;
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
    uint64_t m_num_nonempty_lines;
    uint64_t m_num_empty_lines;
};

struct lines_iterator_wrapper : std::forward_iterator_tag {
    typedef std::string value_type;
    static const uint64_t buf_size = 1024;

    lines_iterator_wrapper(std::ifstream const& ifs) : m_pifs(&ifs) {
        init();
    }

    lines_iterator_wrapper(lines_iterator_wrapper const& rhs) {
        *this = rhs;
    }

    lines_iterator_wrapper& operator=(lines_iterator_wrapper const& rhs) {
        m_pifs = rhs.m_pifs;
        init(); /* NOTE: iteration starts from the beginning of the file. */
        return *this;
    }

    std::string operator*()  //
    {
        m_key.clear();
        if (m_read == m_size) {
            std::stringbuf buffer;
            std::ostream os(&buffer);
            os << "reached end of file";
            os << " after reading " << m_num_nonempty_lines << " non-empty lines";
            throw std::runtime_error(buffer.str());
        }

        while (m_read != m_size) {
            if (m_buf_pos == m_buf.size()) fill_buf();
            if (m_buf[m_buf_pos] == '\n') break;
            m_key.push_back(m_buf[m_buf_pos]);
            m_buf_pos += 1;
            m_read += 1;
        }

        m_buf_pos += 1;
        m_read += 1;
        ++m_num_nonempty_lines;

        if (m_key.length() == 0) {
            ++m_num_empty_lines;
            std::stringbuf buffer;
            std::ostream os(&buffer);
            os << "blank line detected";
            os << " after reading " << m_num_nonempty_lines << " non-empty lines";

            /* NOTE: does not allow more than 1 empty key */
            if (m_num_empty_lines > 1) throw std::runtime_error(buffer.str());
        }

        return m_key;
    }

    void operator++(int) const {}
    void operator++() const {}
    lines_iterator_wrapper operator+(uint64_t) const {
        throw std::runtime_error(
            "lines_iterator_wrapper::operator+(uint64_t) has not been implemented");
    }

private:
    std::ifstream const* m_pifs;
    std::filebuf* m_pbuf;
    std::string m_buf;
    std::string m_key;
    uint64_t m_buf_pos;
    uint64_t m_size;
    uint64_t m_read;
    uint64_t m_num_nonempty_lines;
    uint64_t m_num_empty_lines;

    void fill_buf() {
        assert(m_buf_pos == m_buf.size());
        uint64_t n = buf_size;
        if (m_read + buf_size > m_size) n = m_size - m_read;
        m_buf.resize(n);
        m_pbuf->sgetn(m_buf.data(), n);
        m_buf_pos = 0;
    }

    void init() {
        m_pbuf = m_pifs->rdbuf();
        m_buf_pos = buf_size;
        m_size = 0;
        m_read = 0;
        m_num_nonempty_lines = 0;
        m_num_empty_lines = 0;
        m_size = m_pbuf->pubseekoff(0, m_pifs->end, m_pifs->in);
        m_pbuf->pubseekpos(0, m_pifs->in);
        m_buf.resize(buf_size);
        fill_buf();
    }
};

std::vector<std::string> read_string_collection(uint64_t n, char const* filename, bool verbose) {
    progress_logger logger(n, "read ", " keys from file", verbose);
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
        logger.log();
        if (strings.size() == n) break;
    }
    input.close();
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