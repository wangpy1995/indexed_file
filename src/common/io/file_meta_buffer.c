//
// Created by wangpengyu6 on 18-5-22.
//

#include <string.h>
#include "file_meta_buffer.h"
#include "common/types/meta_data.h"

#define getAs(buffer, num_types, type) ((type *) (get((buffer), (num_types) * sizeof(type))))

static void __attribute__((nonnull(1))) *get(FileMetaBuffer *_this, size_t size) {
    if (_this && size > 0) {
        const char *data = _this->buff + _this->pos;
        _this->pos += size;
        return (void *) data;
    }
    return NULL;
}

////////////////////////////////////////////////
//      read buffer接口
////////////////////////////////////////////////
static void getImmutableStrings(FileMetaBuffer *_this, int32_t num_str, String *str) {
    if (str && num_str > 0) {
        int i;
        for (i = 0; i < num_str; ++i) {
            str[i].length = *getAs(_this, 1, unsigned short);
//            str[i].str = malloc(str[i].length * sizeof(char));
            str->str = getAs(_this, str[i].length, char);
        }
    }
}

static void
getSortingColumns(FileMetaBuffer *_this, unsigned short num_sorting_columns, SortingColumn *sortingColumns) {
    if (sortingColumns && num_sorting_columns > 0) {
        int i;
        for (i = 0; i < num_sorting_columns; ++i) {
            sortingColumns[i].column_idx = *getAs(_this, 1, int32_t);
            sortingColumns[i].descending = *getAs(_this, 1, bool);
            sortingColumns[i].nulls_first = *getAs(_this, 1, bool);
        }
    }
}

static void getPageEncodingStats(FileMetaBuffer *_this, unsigned short num_stats, PageEncodingStats *stats) {
    if (stats) {
        int i;
        for (i = 0; i < num_stats; ++i) {
            stats[i].page_type = *getAs(_this, 1, PageType);
            stats[i].encoding = *getAs(_this, 1, Encoding);
            stats[i].count = *getAs(_this, 1, int32_t);
        }
    }
}

static void getStatistics(FileMetaBuffer *_this, int32_t num_statistics, Statistics *statistics) {
    if (statistics) {
        int i;
        for (i = 0; i < num_statistics; ++i) {
            getImmutableStrings(_this, 1, &(statistics[i].max));
            getImmutableStrings(_this, 1, &(statistics[i].min));
            statistics[i].null_count = *getAs(_this, 1, int64_t);
            statistics[i].distinct_count = *getAs(_this, 1, int64_t);
            getImmutableStrings(_this, 1, &(statistics[i].max_value));
            getImmutableStrings(_this, 1, &(statistics[i].min_value));
        }
    }
}

static void getKeyValues(FileMetaBuffer *_this, int32_t num_kvs, KeyValue *kvs) {
    if (kvs) {
        int i;
        for (i = 0; i < num_kvs; ++i) {
            getImmutableStrings(_this, 1, &(kvs[i].key));
            getImmutableStrings(_this, 1, &(kvs[i].value));
        }
    }
}

static void getColumnMetaDatas(FileMetaBuffer *_this, unsigned short num_metas, ColumnMetaData *columnMetaDatas) {
    if (columnMetaDatas) {
        int i;
        for (i = 0; i < num_metas; ++i) {
            columnMetaDatas[i].type = *getAs(_this, 1, Type);

            columnMetaDatas[i].encoding_len = *getAs(_this, 1, unsigned short);
            if (columnMetaDatas[i].encoding_len > 0) {
                columnMetaDatas[i].encodings = malloc(columnMetaDatas[i].encoding_len * sizeof(Encoding));
                columnMetaDatas[i].encodings = getAs(_this, columnMetaDatas[i].encoding_len, Encoding);
            }

            columnMetaDatas[i].path_len = *getAs(_this, 1, unsigned short);
            if (columnMetaDatas[i].path_len > 0) {
                columnMetaDatas[i].path_in_schema = malloc(columnMetaDatas[i].path_len * sizeof(String));
                getImmutableStrings(_this, columnMetaDatas[i].path_len, columnMetaDatas[i].path_in_schema);
            }
            columnMetaDatas[i].codec = *getAs(_this, 1, CompressionCodec);
            columnMetaDatas[i].num_values = *getAs(_this, 1, int64_t);
            columnMetaDatas[i].total_uncompressed_size = *getAs(_this, 1, int64_t);
            columnMetaDatas[i].total_compressed_size = *getAs(_this, 1, int64_t);

            columnMetaDatas[i].kv_len = *getAs(_this, 1, int32_t);
            if (columnMetaDatas[i].kv_len > 0) {
                columnMetaDatas[i].key_value_metadata = malloc(columnMetaDatas[i].kv_len * sizeof(KeyValue));
                getKeyValues(_this, columnMetaDatas[i].kv_len, columnMetaDatas[i].key_value_metadata);
            }

            columnMetaDatas[i].data_page_offset = *getAs(_this, 1, int64_t);
            columnMetaDatas[i].index_page_offset = *getAs(_this, 1, int64_t);
            columnMetaDatas[i].dictionary_page_offset = *getAs(_this, 1, int64_t);

            getStatistics(_this, 1, &(columnMetaDatas[i].statistics));

            columnMetaDatas[i].stat_len = *getAs(_this, 1, unsigned short);
            if (columnMetaDatas[i].stat_len > 0) {
                columnMetaDatas[i].encoding_stats = malloc(columnMetaDatas[i].stat_len * sizeof(PageEncodingStats));
                getPageEncodingStats(_this, columnMetaDatas[i].stat_len, columnMetaDatas[i].encoding_stats);
            }
        }
    }
}

static void getColumnChunks(FileMetaBuffer *_this, unsigned short num_columns, ColumnChunk *columns) {
    if (columns) {
        int i;
        for (i = 0; i < num_columns; ++i) {
            getImmutableStrings(_this, 1, &(columns[i].file_path));
            columns[i].file_offset = *getAs(_this, 1, int64_t);
            getColumnMetaDatas(_this, 1, &(columns[i].meta_data));
            columns[i].offset_index_offset = *getAs(_this, 1, int64_t);
            columns[i].offset_index_length = *getAs(_this, 1, int32_t);
            columns[i].column_index_offset = *getAs(_this, 1, int64_t);
            columns[i].column_index_length = *getAs(_this, 1, int32_t);
        }
    }
}

static void getRowGroups(FileMetaBuffer *_this, unsigned short num_groups, RowGroup *rowGroups) {
    if (rowGroups) {
        int i;
        for (i = 0; i < num_groups; ++i) {
            rowGroups[i].chunk_len = *getAs(_this, 1, unsigned short);
            if (rowGroups[i].chunk_len > 0) {
                rowGroups[i].columns = malloc(rowGroups[i].chunk_len * sizeof(ColumnChunk));
                getColumnChunks(_this, rowGroups[i].chunk_len, rowGroups[i].columns);
            }
            rowGroups[i].total_byte_size = *getAs(_this, 1, int64_t);
            rowGroups[i].num_rows = *getAs(_this, 1, int64_t);
            rowGroups[i].sorting_columns_len = *getAs(_this, 1, unsigned short);
            if (rowGroups[i].sorting_columns > 0) {
                rowGroups[i].sorting_columns = malloc(rowGroups[i].sorting_columns_len * sizeof(SortingColumn));
                getSortingColumns(_this, rowGroups[i].sorting_columns_len, rowGroups[i].sorting_columns);
            }
        }
    }
}

static void getSchemas(FileMetaBuffer *_this, unsigned short num_schemas, SchemaElement *schemas) {
    if (schemas) {
        int i;
        for (i = 0; i < num_schemas; ++i) {
            //name
            getImmutableStrings(_this, 1, &(schemas[i].name));
            //type
            schemas[i].type = *getAs(_this, 1, Type);

            //type_length
            schemas[i].type_length = *getAs(_this, 1, int32_t);

            //repetition_type
            schemas[i].repetition_type = *getAs(_this, 1, FieldRepetitionType);

            //num_children
            schemas[i].num_children = *getAs(_this, 1, int32_t);

            //convertedType
            schemas[i].converted_type = *getAs(_this, 1, ConvertedType);

            //scale
            schemas[i].scale = *getAs(_this, 1, int32_t);

            //precision
            schemas[i].precision = *getAs(_this, 1, int32_t);

            //field_id
            schemas[i].field_id = *getAs(_this, 1, int32_t);

            //logicalType
            schemas[i].logicalType = *getAs(_this, 1, LogicalType);
        }
    }
}

static FileMetaData *getFileMeta(FileMetaBuffer *_this,int8_t mask) {
    if (_this) {
        FileMetaData *metaData = malloc(sizeof(FileMetaData));
        //version  int32
        metaData->version = *getAs(_this, 1, int32_t);

        //schemas
        if ((mask & SKIP_SCHEMAS) != 0) {
            _this->pos += _this->len[0];
        } else {
            metaData->schema_len = *getAs(_this, 1, unsigned short);
            if (metaData->schema_len > 0) {
                metaData->schema = malloc(metaData->schema_len * sizeof(SchemaElement));
                getSchemas(_this, metaData->schema_len, metaData->schema);
            }
        }
        //numRows int64
        metaData->num_rows = *getAs(_this, 1, int64_t);

        //rowGroups
        if ((mask & SKIP_ROW_GROUPS) != 0) {
            _this->pos += _this->len[1];
        } else {
            metaData->group_len = *getAs(_this, 1, unsigned short);
            if (metaData->group_len > 0) {
                metaData->row_groups = malloc(metaData->schema_len * sizeof(RowGroup));
                getRowGroups(_this, metaData->group_len, metaData->row_groups);
            }
        }

        //kv
        if ((mask & SKIP_KEY_VALUE) != 0) {
            _this->pos += _this->len[2];
        } else {
            metaData->kv_len = *getAs(_this, 1, int32_t);
            if (metaData->kv_len > 0) {
                metaData->key_value_metadata = malloc(metaData->kv_len * sizeof(KeyValue));
                getKeyValues(_this, metaData->kv_len, metaData->key_value_metadata);
            }
        }
        //createBy
        getImmutableStrings(_this, 1, &(metaData->created_by));

        //columnOrders
        metaData->order_len = *getAs(_this, 1, unsigned char);
        if (metaData->order_len > 0) {
            metaData->column_orders = malloc(metaData->order_len * sizeof(ColumnOrder));
            metaData->column_orders = getAs(_this, metaData->order_len, ColumnOrder);
        }
        return metaData;
    }
    return NULL;
}

static void freeFileMeta(FileMetaBuffer *_this) {
    if (_this && _this->fileMeta) {
        FileMetaData *metaData = _this->fileMeta;
        if (metaData->schema) {
            free(metaData->schema);
            metaData->schema = NULL;
        }
        if (metaData->key_value_metadata) {
            free(metaData->key_value_metadata);
            metaData->key_value_metadata = NULL;
        }
        if (metaData->row_groups) {
            if (metaData->row_groups->columns) {
                free(metaData->row_groups->columns);
                metaData->row_groups->columns = NULL;
            }
            if (metaData->row_groups->sorting_columns) {
                free(metaData->row_groups->sorting_columns);
                metaData->row_groups->sorting_columns = NULL;
            }
            free(metaData->row_groups);
            metaData->row_groups = NULL;
        }
        if (metaData->column_orders) {
            free(metaData->column_orders);
            metaData->column_orders = NULL;
        }
        free(metaData);
        _this->fileMeta = NULL;
        _this->pos = 0;
    }
}

FileMetaBuffer *createFileMetaBuffer(const char *buff, int32_t index_len, int8_t mask) {
    FileMetaBuffer *buffer = malloc(sizeof(FileMetaBuffer));
    buffer->pos = 0;
    buffer->buff = buff;
    buffer->len = (int32_t *) (buff + index_len - 3 * sizeof(int32_t));
    buffer->freeFileMeta = freeFileMeta;
    buffer->fileMeta = getFileMeta(buffer,mask);
    return buffer;
}