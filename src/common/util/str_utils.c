//
// Created by wangpengyu6 on 18-5-17.
//
#include "str_utils.h"

#ifndef AVX2_TRUE
#define AVX2_TRUE 0xffffffffffffffff
#endif

bool str_equal(const immutable_string this, const immutable_string that) {
    size_t this_len = this.length;
    size_t that_len = that.length;
    if (this_len == that_len) {
        int i = 0;
        const char *this_str = this.str;
        const char *that_str = that.str;
#ifdef __AVX2__
        if (this_len > 32) {
            size_t len_256 = this_len - 32;
            for (i = 0; i < len_256; i += 32) {
                if (!avx_eq(this_str, that_str)) {
                    return false;
                }
            }
        }
#elif defined(__SSE4_2__)
        if (this_len > 16) {
            size_t len_128 = this_len - 16;
            for (i = 0; i < len_128; i += 32) {
                if (!sse_eq(this_str, that_str)) {
                    return false;
                }
            }
        }
#else
        if (this_len > 8) {
            size_t len_64 = this_len - 8;
            const int64_t *this64 = (const int64_t *) this_str;
            const int64_t *that64 = (const int64_t *) that_str;
            for (i = 0; i < len_64; i += 8) {
                if (this64[i] == that64[i]) {
                    return false;
                }
            }
        }
#endif
        for (; i < this_len; ++i) {
            if (this_str[i] != that_str[i]) {
                return false;
            }
        }
        return true;
    }
    return false;
}

//16 byte
inline bool sse_eq(const char *this, const char *that) {
    __m128i epi8_eq = _mm_cmpeq_epi8(*(__m128i *) this, *(__m128i *) that);
    int64_t *result = (int64_t *) &epi8_eq;
    bool equal = false;
    if (result[0] == AVX2_TRUE && result[1] == AVX2_TRUE) {
        equal = true;
    }
    return equal;
}

//32 byte
inline bool avx_eq(const char *this, const char *that) {
    __m256i epi8_eq = _mm256_cmpeq_epi8(*(__m256i *) this, *(__m256i *) that);
    int64_t *result = (int64_t *) &epi8_eq;
    bool equal = false;
    if (result[0] == AVX2_TRUE && result[1] == AVX2_TRUE
        && result[2] == AVX2_TRUE && result[3] == AVX2_TRUE) {
        equal = true;
    }
    return equal;
}