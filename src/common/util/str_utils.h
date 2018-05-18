//
// Created by wangpengyu6 on 18-5-17.
//

#ifndef INDEXED_FILE_STR_UTILS_H
#define INDEXED_FILE_STR_UTILS_H

#include <immintrin.h>
#include "idx_types.h"

bool str_equal(immutable_string this, immutable_string that);

//16 byte
bool sse_eq(const char *this, const char *that);

//32 byte
bool avx_eq(const char *this, const char *that);

#endif //INDEXED_FILE_STR_UTILS_H
