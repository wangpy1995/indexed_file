//
// Created by wangpengyu6 on 18-5-14.
//

#include <stdlib.h>

#ifndef INDEXED_FILE_IDX_CONSTANTS_H
#define INDEXED_FILE_IDX_CONSTANTS_H

//初始化一些静态变量的值
extern void initConstants(void);

typedef struct immutable_string {
    size_t length;
    const char *str;
} immutable_string;
//静态空string
static const immutable_string empty = {0, ""};

#endif //INDEXED_FILE_IDX_CONSTANTS_H
