//
// Created by wangpengyu6 on 18-5-19.
//

//从文件流中读取FileMetaData

#ifndef INDEXED_FILE_META_DATA_READER_H
#define INDEXED_FILE_META_DATA_READER_H

#include <stdio.h>
#include <common/idx_types.h>


typedef struct MetaDataReader {
    FILE *fp;
    size_t pos;
    // schema | row_group | kv  -> size
    int32_t len[3];

    //mask=0,1,2,3,4,5,6,7
    void (*readFileMeta)(struct MetaDataReader *_this, FileMetaData *metaData, int32_t mask);

    void (*read)(struct MetaDataReader *_this, size_t size, void *res);

    void (*seekTo)(struct MetaDataReader *_this, size_t pos);

    void (*flush)(struct MetaDataReader *_this);

    void (*close)(struct MetaDataReader *_this);
} MetaDataReader;

MetaDataReader *createMetaDataReader(FILE *fp,size_t pos, int index_len);

#endif //INDEXED_FILE_META_DATA_READER_H
