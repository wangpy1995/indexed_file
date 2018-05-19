//
// Created by wangpengyu6 on 18-5-14.
//

#include <stdlib.h>

#ifndef INDEXED_FILE_IDX_CONSTANTS_H
#define INDEXED_FILE_IDX_CONSTANTS_H

//初始化一些静态变量的值
extern void initConstants(void);

//长度短于65536
typedef struct immutable_string {
    unsigned short length;
    //此处建议使用字符串常量  以免free
    const char *str;
} immutable_string;

//静态空string
const static immutable_string EMPTY_STRING = {.str="", .length=0};

#endif //INDEXED_FILE_IDX_CONSTANTS_H
