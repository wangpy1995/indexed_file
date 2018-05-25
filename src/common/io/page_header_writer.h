//
// Created by wangpengyu6 on 18-5-25.
//

#ifndef INDEXED_FILE_PAGE_HEADER_WRITER_H
#define INDEXED_FILE_PAGE_HEADER_WRITER_H

#include <stdio.h>

typedef struct PageHeaderWriter {
    FILE *fp;
    size_t pos;

    void (*seekTo)(struct PageHeaderWriter *_this);

    void (*writePageHeader)(struct PageHeaderWriter *_this);
} PageHeaderWriter;

/**
 *
 * @param fp  文件指针
 * @param pos  偏移量
 */
static PageHeaderWriter *createPageHeaderWriter(FILE *fp, size_t pos);

#endif //INDEXED_FILE_PAGE_HEADER_WRITER_H
