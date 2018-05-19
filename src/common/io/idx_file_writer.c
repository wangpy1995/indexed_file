//
// Created by wangpengyu6 on 18-5-19.
//

#include <stdlib.h>
#include <common/types/meta_data.h>
#include <common/types/schema.h>
#include "idx_file_writer.h"

static void write(IndexedFileWriter *_this, size_t size, void *data);

static void seekTo(IndexedFileWriter *_this, size_t pos);

static void flush(IndexedFileWriter *_this);

static void close(IndexedFileWriter *_this);

static void writeFileMeta(IndexedFileWriter *_this, FileMetaData metaData);

IndexedFileWriter *createIndexedFileWriter(FILE *fp) {
    if (fp) {
        IndexedFileWriter *writer = malloc(sizeof(IndexedFileWriter));
        writer->fp = fp;
        writer->pos = 0;
        writer->writeFileMeta = writeFileMeta;
        writer->write = write;
        writer->seekTo = seekTo;
        writer->flush = flush;
        writer->close = close;
        return writer;
    }
    return NULL;
}

static void write(IndexedFileWriter *_this, size_t size, void *data) {
    if (_this && size > 0 && data) {
        fseek((FILE *) _this->fp, _this->pos, SEEK_SET);
        fwrite(data, size, 1, (FILE *) _this->fp);
        _this->pos += size;
    }
}

static void seekTo(IndexedFileWriter *_this, size_t pos) {
    if (_this) {
        _this->pos = pos;
    }
}

static void flush(IndexedFileWriter *_this) {
    if (_this) {
        fflush(_this->fp);
    }
}

static void close(IndexedFileWriter *_this) {
    if (_this) {
        fclose(_this->fp);
        free(_this);
        _this->fp = NULL;
        _this = NULL;
    }
}

static void writeFileMeta(IndexedFileWriter *_this, const FileMetaData metaData) {
    if (_this) {
        //version  int32
        write(_this, sizeof(int32_t), &(metaData.version));
        //schema_len  int16
        write(_this, sizeof(unsigned short), &(metaData.schema_length));
        write(_this, metaData.schema_length, metaData.schema);
        //numRows int64
        write(_this, sizeof(int64_t), &(metaData.num_rows));
        //rowGroups
        write(_this, sizeof(unsigned short), &(metaData.group_length));
        write(_this, metaData.group_length, metaData.row_groups);
        //kv
        write(_this, sizeof(int32_t), &(metaData.kv_length));
        write(_this, metaData.kv_length, metaData.key_value_metadata);
        //createBy
        write(_this, sizeof(unsigned short), &(metaData.created_by.length));
        write(_this, metaData.created_by.length, metaData.created_by.str);
        //columnOrders
        write(_this, sizeof(unsigned char), &(metaData.column_order_length));
        write(_this, metaData.column_order_length, metaData.column_orders);
    }
}

