#pragma once

#include <xxh3.h>

namespace pthash {

namespace util {

struct high_collision_probability_runtime_error : public std::runtime_error {
    high_collision_probability_runtime_error()
        : std::runtime_error(
              "Using 64-bit hash codes with more than 2^30 keys can be dangerous due to "
              "collisions: use 128-bit hash codes instead.") {}
};

template <typename Hasher>
static inline void check_hash_collision_probability(uint64_t size) {
    /*
        Adapted from: https://preshing.com/20110504/hash-collision-probabilities.
        Given a universe of size U (total number of possible hash values),
        which is U = 2^b for b-bit hash codes,
        the collision probability for n keys is (approximately):
            1 - e^{-n(n-1)/(2U)}.
        For example, for U=2^32 (32-bit hash codes), this probability
        gets to 50% already for n = 77,163 keys.
        We can approximate 1-e^{-X} with X when X is sufficiently small.
        Then our collision probability is
            n(n-1)/(2U) ~ n^2/(2U).
        So it can derived that for ~1.97B keys and 64-bit hash codes,
        the probability of collision is ~0.1 (10%), which may not be
        so small for some applications.
        For n = 2^30, the probability of collision is ~0.031 (3.1%).
    */
    if (sizeof(typename Hasher::hash_type) * 8 == 64 and size > (1ULL << 30)) {
        throw high_collision_probability_runtime_error();
    }
}

}  // namespace util

constexpr inline uint64_t mix(const uint64_t val) {
    return val * 0x517cc1b727220a95;
}

struct hash64 {
    hash64() {}
    hash64(uint64_t hash) : m_hash(hash) {}

    inline uint64_t first() const {
        return m_hash;
    }

    inline uint64_t second() const {
        return m_hash;
    }

    inline uint64_t mix() const {
        return ::pthash::mix(m_hash);
    }

private:
    uint64_t m_hash;
};

struct hash128 {
    hash128() {}
    hash128(XXH128_hash_t xxhash) : m_first(xxhash.high64), m_second(xxhash.low64) {}
    hash128(uint64_t first, uint64_t second) : m_first(first), m_second(second) {}

    inline uint64_t first() const {
        return m_first;
    }

    inline uint64_t second() const {
        return m_second;
    }

    inline uint64_t mix() const {
        return m_first ^ m_second;
    }

private:
    uint64_t m_first, m_second;
};

struct xxhash_64 {
    typedef hash64 hash_type;

    // generic range of bytes
    static inline hash64 hash(uint8_t const* begin, uint8_t const* end, uint64_t seed) {
        return XXH64(begin, end - begin, seed);
    }

    // specialization for std::string
    static inline hash64 hash(std::string const& val, uint64_t seed) {
        return XXH64(val.data(), val.size(), seed);
    }

    // specialization for uint64_t
    static inline hash64 hash(uint64_t const& val, uint64_t seed) {
        return XXH64(&val, sizeof(val), seed);
    }
};

struct xxhash_128 {
    typedef hash128 hash_type;

    // generic range of bytes
    static inline hash128 hash(uint8_t const* begin, uint8_t const* end, uint64_t seed) {
        return XXH128(begin, end - begin, seed);
    }

    // specialization for std::string
    static inline hash128 hash(std::string const& val, uint64_t seed) {
        return XXH128(val.data(), val.size(), seed);
    }

    // specialization for uint64_t
    static inline hash128 hash(uint64_t const& val, uint64_t seed) {
        return XXH128(&val, sizeof(val), seed);
    }
};

}  // namespace pthash