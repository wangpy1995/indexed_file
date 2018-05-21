//
// Created by wangpengyu6 on 18-5-17.
//

#ifndef INDEXED_FILE_STATISTICS_H
#define INDEXED_FILE_STATISTICS_H

#include "idx_types.h"

//Statistics
struct Statistics {
    immutable_string max;
    immutable_string min;
    int64_t null_count;
    int64_t distinct_count;
    immutable_string max_value;
    immutable_string min_value;
};

#endif //INDEXED_FILE_STATISTICS_H
