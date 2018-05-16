
//
// Created by wangpengyu6 on 18-5-11.
//

#include "idx_types.h"
#include <immintrin.h>

#ifndef AVX2_TRUE
#define AVX2_TRUE 0xffffffffffffffff
#endif

bool statistics_equals(const Statistics *This, const Statistics *that);

static Statistics *freeStatistics(Statistics *This);

bool str_equal(immutable_string this, immutable_string that);

//16 byte
inline bool sse_eq(const char *this, const char *that);

//32 byte
inline bool avx_eq(const char *this, const char *that);

void statistics_to_str(const Statistics *This,char *str);

Statistics *initStatistics() {
    return initStatistics_(empty, empty, 0, 0, empty, empty);
}

Statistics *initStatistics_(immutable_string max, immutable_string min, int64_t nullCount, int64_t distinctCount,
                            immutable_string maxValue, immutable_string minValue) {
    if (&nullCount == NULL || nullCount < 0 || &distinctCount == NULL || distinctCount < 0) {
        return NULL;
    }
    Statistics *s = malloc(sizeof(Statistics));
    s->This = s;
    s->max = max;
    s->min = min;
    s->null_count = nullCount;
    s->distinct_count = distinctCount;
    s->max_value = maxValue;
    s->min_value = minValue;
    s->equals = statistics_equals;
    s->free = freeStatistics;
    s->toString = statistics_to_str;
    return s;
}

static Statistics *freeStatistics(Statistics *This) {
    if (This) {
        printf("free statistics....\n");
        free(This);
        This = NULL;
    }
    return This;
}

bool statistics_equals(const Statistics *This, const Statistics *that) {
    //TODO max, min, max_value, min_value 并行比较
    if (This == NULL && that == NULL) {
        return true;
    } else if (This != NULL && that != NULL) {
        if (This->This == that->This) {
            return true;
        } else {
            return (str_equal(This->max, that->max)
                    && str_equal(This->min, that->min)
                    && This->null_count == that->null_count
                    && This->distinct_count == that->distinct_count
                    && str_equal(This->min_value, that->min_value)
                    && str_equal(This->max_value, that->max_value));
        }
    } else {
        return false;
    }
}

void statistics_to_str(const Statistics *This, char *str) {
    if (This) {
        size_t len = 6 + This->max.length + 6 + This->min.length + 7 + 20 + 11 + 20 + 12 + This->max_value.length + 12 +
                     This->min_value.length + 2;
//        char result[len];
        sprintf(str, "{max: %s, min: %s, null: %ld, distinct: %ld,max_value: %s,min_value:  %s}",
                This->max.str, This->min.str, This->null_count,
                This->distinct_count, This->max_value.str, This->min_value.str);
    }
}

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
    } else {
        return false;
    }
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