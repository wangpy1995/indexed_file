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

    bool (*equals)(const Statistics *_this, const Statistics *that);

    //将字符串写入到s中
    char *(*mkString)(const Statistics *_this, char *str);

    Statistics *(*free)(Statistics *_this);
};

static Statistics STATISTICS;

Statistics *initStatistics(immutable_string max, immutable_string min, int64_t nullCount, int64_t distinctCount,
                           immutable_string maxValue, immutable_string minValue);

#endif //INDEXED_FILE_STATISTICS_H
