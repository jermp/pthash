#pragma once

#include <cassert>
#include <cstdint>

#if defined(__x86_64__)
#include <immintrin.h>
#endif

namespace pthash::util {

inline uint8_t msb(uint64_t x) {
    assert(x);
    unsigned long ret = -1U;
    if (x) { ret = (unsigned long)(63 - __builtin_clzll(x)); }
    return (uint8_t)ret;
}

inline bool bsr64(unsigned long* const index, const uint64_t mask) {
    if (mask) {
        *index = (unsigned long)(63 - __builtin_clzll(mask));
        return true;
    } else {
        return false;
    }
}

inline uint8_t msb(uint64_t x, unsigned long& ret) {
    return bsr64(&ret, x);
}

inline uint8_t lsb(uint64_t x, unsigned long& ret) {
    if (x) {
        ret = (unsigned long)__builtin_ctzll(x);
        return true;
    }
    return false;
}

inline uint8_t lsb(uint64_t x) {
    assert(x);
    unsigned long ret = -1U;
    lsb(x, ret);
    return (uint8_t)ret;
}

inline uint64_t popcount(uint64_t x) {
#ifdef __SSE4_2__
    return static_cast<uint64_t>(_mm_popcnt_u64(x));
#elif __cplusplus >= 202002L
    return std::popcount(x);
#else
    return static_cast<uint64_t>(__builtin_popcountll(x));
#endif
}

inline uint64_t select64(uint64_t x, uint64_t k) {
#ifndef __BMI2__
    // Modified from: Bit Twiddling Hacks
    // https://graphics.stanford.edu/~seander/bithacks.html#SelectPosFromMSBRank
    unsigned int s;       // Output: Resulting position of bit with rank r [1-64]
    uint64_t a, b, c, d;  // Intermediate temporaries for bit count.
    unsigned int t;       // Bit count temporary.
    k = popcount(x) - k;

    a = x - ((x >> 1) & ~0UL / 3);
    b = (a & ~0UL / 5) + ((a >> 2) & ~0UL / 5);
    c = (b + (b >> 4)) & ~0UL / 0x11;
    d = (c + (c >> 8)) & ~0UL / 0x101;
    t = (d >> 32) + (d >> 48);
    s = 64;
    s -= ((t - k) & 256) >> 3;
    k -= (t & ((t - k) >> 8));
    t = (d >> (s - 16)) & 0xff;
    s -= ((t - k) & 256) >> 4;
    k -= (t & ((t - k) >> 8));
    t = (c >> (s - 8)) & 0xf;
    s -= ((t - k) & 256) >> 5;
    k -= (t & ((t - k) >> 8));
    t = (b >> (s - 4)) & 0x7;
    s -= ((t - k) & 256) >> 6;
    k -= (t & ((t - k) >> 8));
    t = (a >> (s - 2)) & 0x3;
    s -= ((t - k) & 256) >> 7;
    k -= (t & ((t - k) >> 8));
    t = (x >> (s - 1)) & 0x1;
    s -= ((t - k) & 256) >> 8;
    return s - 1;
#else
    uint64_t i = 1ULL << k;
    asm("pdep %[x], %[mask], %[x]" : [ x ] "+r"(x) : [ mask ] "r"(i));
    asm("tzcnt %[bit], %[index]" : [ index ] "=r"(i) : [ bit ] "g"(x) : "cc");
    return i;
#endif
}

inline uint64_t select_in_word(const uint64_t x, const uint64_t k) {
    assert(k < popcount(x));
    return select64(x, k);
}

}  // namespace pthash::util