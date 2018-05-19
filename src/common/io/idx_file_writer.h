//
// Created by wangpengyu6 on 18-5-19.
//

#ifndef INDEXED_FILE_IDX_FILE_WRITER_H
#define INDEXED_FILE_IDX_FILE_WRITER_H

#include <stdio.h>
#include <common/idx_types.h>

typedef struct IndexedFileWriter {
    const FILE *fp;
    size_t pos;

    void (*writeFileMeta)(struct IndexedFileWriter *_this, const FileMetaData metaData);

    void (*write)(struct IndexedFileWriter *_this, size_t size, void *data);

    void (*seekTo)(struct IndexedFileWriter *_this, size_t pos);

    void (*flush)(struct IndexedFileWriter *_this);

    void (*close)(struct IndexedFileWriter *_this);
} IndexedFileWriter;

IndexedFileWriter *createIndexedFileWriter(FILE *fp);

#endif //INDEXED_FILE_IDX_FILE_WRITER_H
