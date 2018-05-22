//
// Created by wangpengyu6 on 18-5-19.
//

#ifndef INDEXED_FILE_META_DATA_READER_H
#define INDEXED_FILE_META_DATA_READER_H

#include <stdio.h>
#include <common/idx_types.h>

typedef struct MetaDataReader {
    FILE *fp;
    size_t pos;

    void (*readFileMeta)(struct MetaDataReader *_this, FileMetaData *metaData);

    void (*read)(struct MetaDataReader *_this, size_t size, void *res);

    void (*seekTo)(struct MetaDataReader *_this, size_t pos);

    void (*flush)(struct MetaDataReader *_this);

    void (*close)(struct MetaDataReader *_this);
} MetaDataReader;

MetaDataReader *createMetaDataReader(FILE *fp);

#endif //INDEXED_FILE_META_DATA_READER_H
