//
// Created by wangpengyu6 on 18-5-19.
//

//将FileMetaData中的实际数据写入到文件中

#ifndef INDEXED_FILE_META_DATA_WRITER_H
#define INDEXED_FILE_META_DATA_WRITER_H

#include <stdio.h>
#include <common/idx_types.h>

typedef struct MetaDataWriter {
    FILE *fp;
    size_t pos;

    void (*writeFileMeta)(struct MetaDataWriter *_this, const FileMetaData metaData);

    void (*writePageHeader)(struct MetaDataWriter *_this, const PageHeader pageHeader);

    void (*write)(struct MetaDataWriter *_this, size_t size, const void *data);

    void (*seekTo)(struct MetaDataWriter *_this, size_t pos);

    void (*flush)(struct MetaDataWriter *_this);

    void (*close)(struct MetaDataWriter *_this);
} MetaDataWriter;

MetaDataWriter *createMetaDataWriter(FILE *fp);

#endif //INDEXED_FILE_META_DATA_WRITER_H
