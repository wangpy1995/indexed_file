//
// Created by wangpengyu6 on 18-5-17.
//

#ifndef INDEXED_FILE_STATISTICS_H
#define INDEXED_FILE_STATISTICS_H

#include "idx_types.h"

//Statistics  列數據的一些幾處屬性
struct Statistics {
    String max;
    String min;
    int64_t null_count;
    int64_t distinct_count;
    String max_value;
    String min_value;
};

#endif //INDEXED_FILE_STATISTICS_H
