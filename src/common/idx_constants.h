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
    //此处建议使用字符串常量  以免free
    const char *str;
} immutable_string;
//静态空string
const immutable_string EMPTY_STRING = {0, ""};

#endif //INDEXED_FILE_IDX_CONSTANTS_H
