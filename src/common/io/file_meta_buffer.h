//
// Created by wangpengyu6 on 18-5-22.
//

#ifndef INDEXED_FILE_FILE_META_BUFFER_H
#define INDEXED_FILE_FILE_META_BUFFER_H

#include <common/idx_types.h>

typedef struct FileMetaBuffer {
    void (*readFileMeta)(struct FileMetaBuffer *buff, FileMetaData *meta);

    void (*seekTo)(struct FileMetaBuffer *buff, size_t offset);

    size_t pos;
    const char *buff;
} FileMetaBuffer;

FileMetaBuffer *createFileMetaBuffer(const char *buffer);

#endif //INDEXED_FILE_FILE_META_BUFFER_H
