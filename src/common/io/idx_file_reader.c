//
// Created by wangpengyu6 on 18-5-19.
//

#include <stdlib.h>
#include <common/types/meta_data.h>
#include "idx_file_reader.h"

static void read(struct IndexedFileReader *_this, size_t size, void *res);

static void seekTo(struct IndexedFileReader *_this, size_t pos);

static void flush(IndexedFileReader *_this);

static void close(struct IndexedFileReader *_this);

const static FileMetaData *readFileMeta(IndexedFileReader *_this);

IndexedFileReader *createIndexedFileReader(FILE *fp) {
    if (fp) {
        IndexedFileReader *reader = malloc(sizeof(IndexedFileReader));
        reader->fp = fp;
        reader->pos = 0;
        reader->readFileMeta = readFileMeta;
        reader->read = read;
        reader->seekTo = seekTo;
        reader->flush = flush;
        reader->close = close;
        return reader;
    }
    return NULL;
}

static void read(struct IndexedFileReader *_this, size_t size, void *res) {
    if (_this && size > 0 && res) {
        fseek(_this->fp, _this->pos, SEEK_SET);
        fread(res, size, 1, _this->fp);
        _this->pos += size;
    }
}

static void seekTo(struct IndexedFileReader *_this, size_t pos) {
    if (_this && pos > 0) {
        _this->pos = pos;
    }
}

static void flush(IndexedFileReader *_this) {
    if (_this) {
        fflush(_this->fp);
    }
}

static void close(struct IndexedFileReader *_this) {
    if (_this) {
        fclose(_this->fp);
        free(_this);
        _this->fp = NULL;
    }
}

const static FileMetaData *readFileMeta(IndexedFileReader *_this) {
    if (_this) {
        FileMetaData *metaData = malloc(sizeof(FileMetaData));
        //version  int32
        read(_this, sizeof(int32_t), &(metaData->version));

        //schemas
        read(_this, sizeof(unsigned short), &(metaData->schema_length));
        if (metaData->schema_length > 0) {
            metaData->schema = malloc(metaData->schema_length);
            read(_this, metaData->schema_length, metaData->schema);
        }
        //numRows int64
        read(_this, sizeof(int64_t), &(metaData->num_rows));

        //rowGroups
        read(_this, sizeof(unsigned short), &(metaData->group_length));
        if (metaData->group_length > 0) {
            metaData->row_groups = malloc(metaData->group_length);
            read(_this, metaData->group_length, metaData->row_groups);
        }

        //kv
        read(_this, sizeof(int32_t), &(metaData->kv_length));
        if (metaData->kv_length > 0) {
            metaData->key_value_metadata = malloc(metaData->kv_length);
            read(_this, metaData->kv_length, metaData->key_value_metadata);
        }
        //createBy
        read(_this, sizeof(unsigned short), &(metaData->created_by.length));
        if (metaData->created_by.length > 0) {
            metaData->created_by.str = malloc(metaData->created_by.length);
            read(_this, metaData->created_by.length, metaData->created_by.str);
        }
        //columnOrders
        read(_this, sizeof(unsigned char), &(metaData->column_order_length));
        if (metaData->column_order_length > 0) {
            metaData->column_orders = malloc(metaData->column_order_length);
            read(_this, metaData->column_order_length, metaData->column_orders);
        }
        return metaData;
    } else {
        return NULL;
    }
}