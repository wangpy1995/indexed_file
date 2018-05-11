
//
// Created by wangpengyu6 on 18-5-11.
//

#include "idx_type.h"

Statistics *initStatistics() {
    Statistics *s = malloc(sizeof(Statistics));
    s->This = s;
    s->max = "";
    s->min = "";
    s->null_count = 0;
    s->distinct_count = 0;
    s->max_value = "";
    s->min_value = "";
    s->equals = statistics_equals;
    return s;
}

bool statistics_equals(const Statistics *This, const Statistics *that) {
    return (strcmp(This->max, that->max) == 0
            && strcmp(This->min, that->min) == 0
            && This->null_count == that->null_count
            && This->distinct_count == that->distinct_count
            && strcmp(This->min_value, that->min_value) == 0
            && strcmp(This->max_value, that->max_value) == 0);
}

inline bool rest_eq(const char *this, const char *that, const size_t start, size_t size) {
    for (size_t i = start; i < size; ++i) {
        if (this[i] != that[i]) {
            return false;
        }
    }
    return true;
}
inline bool int64_eq(const int64_t *this, const int64_t *that, const size_t len64) {
    for (int i = 0; i < len64; ++i) {
        if (this[i] != that[i]) {
            return false;
        }
    }
    return true;
}

bool fast_equal(const char *this, const char *that) {
    size_t this_len = strlen(this);
    size_t that_len = strlen(that);
    if (this_len == that_len) {
        if(this_len>8){
            size_t rest_len = this_len&0x7;
            size_t len_64 = this_len-rest_len;
            if(int64_eq((const int64_t *) this, (const int64_t *) that, len_64)){
                return rest_eq(this,that,len_64,rest_len);
            }else{
                return false;
            }
        }else{
            return rest_eq(this,that,0,this_len);
        }
    } else {
        return false;
    }
}

bool avx_eq(const char *this, const char *that) {
    return false;
}

bool sse_eq(const char *this, const char *that) {
    return false;
}