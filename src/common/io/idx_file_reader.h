//
// Created by wangpengyu6 on 18-5-19.
//

#ifndef INDEXED_FILE_IDX_FILE_READER_H
#define INDEXED_FILE_IDX_FILE_READER_H

#include <stdio.h>
#include <common/idx_types.h>

typedef struct IndexedFileReader {
    FILE *fp;
    size_t pos;

    const FileMetaData* (*readFileMeta)(struct IndexedFileReader *_this);

    void (*read)(struct IndexedFileReader *_this, size_t size, void *res);

    void (*seekTo)(struct IndexedFileReader *_this, size_t pos);

    void (*flush)(struct IndexedFileReader *_this);

    void (*close)(struct IndexedFileReader *_this);
} IndexedFileReader;

IndexedFileReader *createIndexedFileReader(FILE *fp);

#endif //INDEXED_FILE_IDX_FILE_READER_H
