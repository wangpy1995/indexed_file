//
// Created by wangpengyu6 on 18-5-22.
//

//通过移动指针从共享内存中构造FileMetaData 数据不做拷贝

#ifndef INDEXED_FILE_FILE_META_BUFFER_H
#define INDEXED_FILE_FILE_META_BUFFER_H

#include <common/idx_types.h>

/**
 * @param buff FileMeta的所有内容  需要包含MetaData的全部信息
 */
typedef struct FileMetaBuffer {
    FileMetaData *fileMeta;

    void (*freeFileMeta)(struct FileMetaBuffer *buffer);

    void (*seekTo)(struct FileMetaBuffer *buff, size_t offset);

    size_t pos;
    int32_t *len;
    const char *buff;
} FileMetaBuffer;

FileMetaBuffer *createFileMetaBuffer(const char *buffer,int32_t index_len,int8_t mask);

#endif //INDEXED_FILE_FILE_META_BUFFER_H
