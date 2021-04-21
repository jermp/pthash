#pragma once

#include <fstream>
#include <thread>

namespace pthash {

typedef uint32_t bucket_id_type;

#pragma pack(push, 1)
struct record {
    uint8_t bucket_size;
    bucket_id_type bucket_id;
    uint64_t hash;
};
#pragma pack(pop)

struct record_comparator {
    bool operator()(record const& x, record const& y) const {
        if (x.bucket_size != y.bucket_size) return x.bucket_size > y.bucket_size;
        if (x.bucket_id != y.bucket_id) return x.bucket_id < y.bucket_id;
        return x.hash < y.hash;
    }
};

struct meta_block {
    meta_block(std::string const& filename, uint64_t num_records)
        : m_filename(filename)
        , m_num_records(num_records)
        , m_read_records(num_records)
        , m_eof(true) {}

    void open() {
        m_read_records = 0;
        m_eof = false;
        m_in.open(m_filename.c_str(), std::ifstream::binary);
    }

    void close() {
        if (m_in.is_open()) m_in.close();
    }

    void close_and_remove() {
        close();
        std::remove(m_filename.c_str());
    }

    bool eof() const {
        return m_eof;
    }

    void load(uint64_t num_records) {
        if (eof()) return;
        if (m_read_records + num_records >= m_num_records) {
            num_records = m_num_records - m_read_records;
            m_eof = true;
        }
        m_buffer.resize(num_records);
        m_in.read(reinterpret_cast<char*>(m_buffer.data()),
                  static_cast<std::streamsize>(num_records * sizeof(record)));
        m_read_records += num_records;
    }

    uint64_t num_records() const {
        return m_num_records;
    }

    std::vector<record>& buffer() {
        return m_buffer;
    }

    std::string const& filename() const {
        return m_filename;
    }

    void release() {
        std::vector<record>().swap(m_buffer);
    }

private:
    std::string m_filename;
    uint64_t m_num_records;
    uint64_t m_read_records;
    std::vector<record> m_buffer;
    std::ifstream m_in;
    bool m_eof;
};

typedef std::vector<record>::iterator record_iterator;

struct cursor {
    cursor(record_iterator begin, record_iterator end, uint64_t id)
        : begin(begin), end(end), id(id) {}
    record_iterator begin, end;
    uint64_t id;
};

struct cursor_comparator {
    bool operator()(cursor const& l, cursor const& r) {
        return !record_comparator()(*(l.begin), *(r.begin));
    }
};

template <typename T, typename Comparator>
struct heap {
    void push(T const& t) {
        m_q.push_back(t);
        std::push_heap(m_q.begin(), m_q.end(), m_comparator);
    }

    T& top() {
        return m_q.front();
    }

    void pop() {
        std::pop_heap(m_q.begin(), m_q.end(), m_comparator);
        m_q.pop_back();
    }

    void heapify() {
        uint64_t pos = 0;
        while (2 * pos + 1 < size()) {
            uint64_t i = 2 * pos + 1;
            if (i + 1 < size() and m_comparator(m_q[i], m_q[i + 1])) ++i;
            if (!m_comparator(m_q[pos], m_q[i])) break;
            std::swap(m_q[pos], m_q[i]);
            pos = i;
        }
    }

    bool empty() const {
        return m_q.empty();
    }

    inline uint64_t size() const {
        return m_q.size();
    }

private:
    std::vector<T> m_q;
    Comparator m_comparator;
};

template <typename Funct, typename... Args>
auto async(Funct& f, Args&&... args) {
    return std::make_unique<std::thread>(f, args...);
}

void wait(std::unique_ptr<std::thread>& handle) {
    if (handle != nullptr and handle->joinable()) handle->join();
}

struct parallel_record_sorter {
    parallel_record_sorter(uint64_t max_bucket_size,
                           uint64_t num_threads = std::thread::hardware_concurrency())
        : m_max_bucket_size(max_bucket_size), m_num_threads(num_threads) {
        if (m_num_threads == 0) throw std::runtime_error("number of threads must be > 0");
    }

    void sort(std::vector<record>& records) const {
        std::vector<std::vector<uint64_t>> counts(m_num_threads,
                                                  std::vector<uint64_t>(m_max_bucket_size, 0));
        uint64_t partition_size = records.size() / m_num_threads;
        if (partition_size == 0) throw std::runtime_error("too many threads");

        std::vector<std::thread> threads(m_num_threads);

        // count
        for (uint64_t i = 0; i != m_num_threads; ++i) {
            threads[i] = std::thread(
                [&](uint64_t i) {
                    auto b = records.begin() + i * partition_size;
                    auto e = b + partition_size;
                    if (i == m_num_threads - 1) e = records.end();
                    std::for_each(b, e, [&](record const& r) {
                        uint8_t bucket_size = r.bucket_size;
                        assert(bucket_size <= m_max_bucket_size);
                        ++counts[i][m_max_bucket_size - bucket_size];
                    });
                },
                i);
        }
        for (auto& t : threads) {
            if (t.joinable()) t.join();
        }

        for (uint64_t j = 0, sum = 0; j != m_max_bucket_size; ++j) {
            for (uint64_t i = 0; i != m_num_threads; ++i) {
                uint64_t occ = counts[i][j];
                counts[i][j] = sum;
                sum += occ;
            }
        }

        std::vector<uint64_t> offsets;
        {
            offsets.reserve(m_num_threads + 1);
            offsets.push_back(0);
            auto const& p = counts.front();
            uint64_t target = partition_size;
            uint64_t i = 0;
            while (target < records.size()) {
                while (i < p.size() and p[i] < target) ++i;
                if (p[i - 1] != offsets.back()) offsets.push_back(p[i - 1]);
                target += partition_size;
            }
            offsets.push_back(records.size());
        }

        // std::cout << "offsets" << std::endl;
        // for (auto x : offsets) std::cout << x << " ";
        // std::cout << std::endl;

        std::vector<record> tmp(records.size());

        // displace
        for (uint64_t i = 0; i != m_num_threads; ++i) {
            threads[i] = std::thread(
                [&](uint64_t i) {
                    auto b = records.begin() + i * partition_size;
                    auto e = b + partition_size;
                    if (i == m_num_threads - 1) e = records.end();
                    auto& partition_counts = counts[i];
                    std::for_each(b, e, [&](record const& r) {
                        uint8_t bucket_size = r.bucket_size;
                        assert(bucket_size <= m_max_bucket_size);
                        tmp[partition_counts[m_max_bucket_size - bucket_size]++] = r;
                    });
                },
                i);
        }
        for (auto& t : threads) {
            if (t.joinable()) t.join();
        }

        // sort
        threads.resize(offsets.size() - 1);
        for (uint64_t i = 0; i != offsets.size() - 1; ++i) {
            threads[i] = std::thread(
                [&](uint64_t begin, uint64_t end) {
                    auto b = tmp.begin() + begin;
                    auto e = tmp.begin() + end;
                    std::sort(b, e, record_comparator());
                },
                offsets[i], offsets[i + 1]);
        }
        for (auto& t : threads) {
            if (t.joinable()) t.join();
        }

        records.swap(tmp);
        assert(std::is_sorted(records.begin(), records.end(), record_comparator()));
    }

private:
    uint64_t m_max_bucket_size;
    uint64_t m_num_threads;
};
}  // namespace pthash
