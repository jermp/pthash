#pragma once

// See also https://github.com/jermp/bench_hash_functions

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

struct byte_range {
    uint8_t const* begin;
    uint8_t const* end;
};

/*
    This code is an adaptation from
    https://github.com/aappleby/smhasher/blob/master/src/MurmurHash2.cpp
        by Austin Appleby
*/
static uint64_t MurmurHash2_64(void const* key, size_t len, uint64_t seed) {
    const uint64_t m = 0xc6a4a7935bd1e995ULL;
    const int r = 47;

    uint64_t h = seed ^ (len * m);

#if defined(__arm) || defined(__arm__)
    const size_t ksize = sizeof(uint64_t);
    const unsigned char* data = (const unsigned char*)key;
    const unsigned char* end = data + (std::size_t)(len / 8) * ksize;
#else
    const uint64_t* data = (const uint64_t*)key;
    const uint64_t* end = data + (len / 8);
#endif

    while (data != end) {
#if defined(__arm) || defined(__arm__)
        uint64_t k;
        memcpy(&k, data, ksize);
        data += ksize;
#else
        uint64_t k = *data++;
#endif

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const unsigned char* data2 = (const unsigned char*)data;

    switch (len & 7) {
        // fall through
        case 7:
            h ^= uint64_t(data2[6]) << 48;
        // fall through
        case 6:
            h ^= uint64_t(data2[5]) << 40;
        // fall through
        case 5:
            h ^= uint64_t(data2[4]) << 32;
        // fall through
        case 4:
            h ^= uint64_t(data2[3]) << 24;
        // fall through
        case 3:
            h ^= uint64_t(data2[2]) << 16;
        // fall through
        case 2:
            h ^= uint64_t(data2[1]) << 8;
        // fall through
        case 1:
            h ^= uint64_t(data2[0]);
            h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

inline uint64_t default_hash64(uint64_t val, uint64_t seed) {
    return MurmurHash2_64(&val, sizeof(uint64_t), seed);
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
        // From: http://zimbry.blogspot.com/2011/09/better-bit-mixing-improving-on.html
        // 13-th variant
        uint64_t z = m_hash;
        z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
        z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
        return z ^ (z >> 31);
    }

private:
    uint64_t m_hash;
};

struct hash128 {
    hash128() {}
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

struct murmurhash2_64 {
    typedef hash64 hash_type;

    // generic range of bytes
    static inline hash64 hash(byte_range range, uint64_t seed) {
        return MurmurHash2_64(range.begin, range.end - range.begin, seed);
    }

    // specialization for std::string
    static inline hash64 hash(std::string const& val, uint64_t seed) {
        return MurmurHash2_64(val.data(), val.size(), seed);
    }

    // specialization for uint64_t
    static inline hash64 hash(uint64_t val, uint64_t seed) {
        return MurmurHash2_64(reinterpret_cast<char const*>(&val), sizeof(val), seed);
    }
};

struct murmurhash2_128 {
    typedef hash128 hash_type;

    // generic range of bytes
    static inline hash128 hash(byte_range range, uint64_t seed) {
        return {MurmurHash2_64(range.begin, range.end - range.begin, seed),
                MurmurHash2_64(range.begin, range.end - range.begin, ~seed)};
    }

    // specialization for std::string
    static inline hash128 hash(std::string const& val, uint64_t seed) {
        return {MurmurHash2_64(val.data(), val.size(), seed),
                MurmurHash2_64(val.data(), val.size(), ~seed)};
    }

    // specialization for uint64_t
    static inline hash128 hash(uint64_t val, uint64_t seed) {
        return {MurmurHash2_64(reinterpret_cast<char const*>(&val), sizeof(val), seed),
                MurmurHash2_64(reinterpret_cast<char const*>(&val), sizeof(val), ~seed)};
    }
};

}  // namespace pthash