//
// Created by wangpengyu6 on 18-5-17.
//

#include "statistics.h"
#include "util/str_utils.h"

static bool equals(const Statistics *_this, const Statistics *that);

static char *mkString(const Statistics *_this, char *str);

static Statistics *freeStatistics(Statistics *_this);

Statistics *initStatistics(immutable_string max, immutable_string min, int64_t nullCount, int64_t distinctCount,
                           immutable_string maxValue, immutable_string minValue) {
    if (nullCount < 0 || distinctCount < 0) {
        printf("init Statistics error, invalid count, null [%ld], distinct [%ld].\n", nullCount, distinctCount);
        return NULL;
    }
    Statistics *s = malloc(sizeof(Statistics));
    s->max = max;
    s->min = min;
    s->null_count = nullCount;
    s->distinct_count = distinctCount;
    s->max_value = maxValue;
    s->min_value = minValue;
    s->equals = equals;
    s->free = freeStatistics;
    s->mkString = mkString;
    return s;
}

static bool equals(const Statistics *_this, const Statistics *that) {
    //TODO max, min, max_value, min_value 并行比较
    if (_this == that) {
        return true;
    } else if (_this && that) {
        return (str_equal(_this->max, that->max)
                && str_equal(_this->min, that->min)
                && _this->null_count == that->null_count
                && _this->distinct_count == that->distinct_count
                && str_equal(_this->min_value, that->min_value)
                && str_equal(_this->max_value, that->max_value));
    } else {
        return false;
    }
}

static char *mkString(const Statistics *_this, char *str) {
    if (_this && str) {
        sprintf(str, "Statistics@%p, {max: %s, min: %s, null: %ld, distinct: %ld,max_value: %s,min_value:  %s}", _this,
                _this->max.str, _this->min.str, _this->null_count,
                _this->distinct_count, _this->max_value.str, _this->min_value.str);
    }
    return str;
}

static Statistics *freeStatistics(Statistics *_this) {
    if (_this) {
        free(_this);
        printf("free Statistics@%p success.\n", _this);
        _this = NULL;
    }
    return _this;
}
