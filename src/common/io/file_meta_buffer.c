//
// Created by wangpengyu6 on 18-5-22.
//

#include "file_meta_buffer.h"
#include "common/types/meta_data.h"

static void readFileMeta(FileMetaBuffer *buff, FileMetaData *meta);

static void readImmutableStrings(FileMetaBuffer *_this, int32_t num_str, immutable_string *str);

static void readSchemas(FileMetaBuffer *_this, unsigned short num_schemas, SchemaElement *schemas);

static void readKeyValues(FileMetaBuffer *_this, int32_t num_kvs, KeyValue *kvs);

static void readStatistics(FileMetaBuffer *_this, int32_t num_statistics, Statistics *statistics);

static void readPageEncodingStats(FileMetaBuffer *_this, unsigned short num_stats, PageEncodingStats *stats);

static void readColumnMetaDatas(FileMetaBuffer *_this, unsigned short num_metas, ColumnMetaData *columnMetaDatas);

static void
readSortingColumns(FileMetaBuffer *_this, unsigned short num_sorting_columns, SortingColumn *sortingColumns);

static void readColumnChunks(FileMetaBuffer *_this, unsigned short num_columns, ColumnChunk *columns);

static void readRowGroups(FileMetaBuffer *_this, unsigned short num_groups, RowGroup *rowGroups) ;

FileMetaBuffer *createFileMetaBuffer(const char *buff) {
    FileMetaBuffer *buffer = malloc(sizeof(FileMetaBuffer));
    buffer->pos = 0;
    buffer->buff = buff;
    buffer->readFileMeta=readFileMeta;
    return buffer;
}

static void *read(FileMetaBuffer *_this, size_t size) {
    if (_this && size > 0) {
        const char *data = _this->buff + _this->pos;
        _this->pos += size;
        return (void *) data;
    }
    return NULL;
}

#define readAs(buffer, num_types, type) ((type *) (read((buffer), (num_types) * sizeof(type))))

static void readFileMeta(FileMetaBuffer *_this, FileMetaData *metaData) {
    if (_this && metaData) {
        //version  int32
        metaData->version = *readAs(_this, 1, int32_t);

        //schemas
        metaData->num_schemas = *readAs(_this, 1, unsigned short);
        if (metaData->num_schemas > 0) {
            metaData->schema = malloc(metaData->num_schemas * sizeof(SchemaElement));
            readSchemas(_this, metaData->num_schemas, metaData->schema);
        }
        //numRows int64
        metaData->num_rows = *readAs(_this, 1, int64_t);

        //rowGroups
        metaData->num_groups = *readAs(_this, 1, unsigned short);
        if (metaData->num_groups > 0) {
            metaData->row_groups = malloc(metaData->num_schemas * sizeof(RowGroup));
            readRowGroups(_this, metaData->num_groups, metaData->row_groups);
        }

        //kv
        metaData->num_kvs = *readAs(_this, 1, int32_t);
        if (metaData->num_kvs > 0) {
            metaData->key_value_metadata = malloc(metaData->num_kvs * sizeof(KeyValue));
            readKeyValues(_this, metaData->num_kvs, metaData->key_value_metadata);
        }
        //createBy
        readImmutableStrings(_this, 1, &(metaData->created_by));

        //columnOrders
        metaData->num_column_orders = *readAs(_this, 1, unsigned char);
        if (metaData->num_column_orders > 0) {
            metaData->column_orders = malloc(metaData->num_column_orders * sizeof(ColumnOrder));
            metaData->column_orders = readAs(_this, metaData->num_column_orders, ColumnOrder);
        }
    }
}

static void readSchemas(FileMetaBuffer *_this, unsigned short num_schemas, SchemaElement *schemas) {
    if (schemas) {
        int i;
        for (i = 0; i < num_schemas; ++i) {
            //name
            readImmutableStrings(_this, 1, &(schemas[i].name));
            //type
            schemas[i].type = *readAs(_this, 1, Type);

            //type_length
            schemas[i].type_length = *readAs(_this, 1, int32_t);

            //repetition_type
            schemas[i].repetition_type = *readAs(_this, 1, FieldRepetitionType);

            //num_children
            schemas[i].num_children = *readAs(_this, 1, int32_t);

            //convertedType
            schemas[i].converted_type = *readAs(_this, 1, ConvertedType);

            //scale
            schemas[i].scale = *readAs(_this, 1, int32_t);

            //precision
            schemas[i].precision = *readAs(_this, 1, int32_t);

            //field_id
            schemas[i].field_id = *readAs(_this, 1, int32_t);

            //logicalType
            schemas[i].logicalType = *readAs(_this, 1, LogicalType);
        }
    }
}

static void readRowGroups(FileMetaBuffer *_this, unsigned short num_groups, RowGroup *rowGroups) {
    if (rowGroups) {
        int i;
        for (i = 0; i < num_groups; ++i) {
            rowGroups[i].num_columns = *readAs(_this, 1, unsigned short);
            if (rowGroups[i].num_columns > 0) {
                rowGroups[i].columns = malloc(rowGroups[i].num_columns * sizeof(ColumnChunk));
                readColumnChunks(_this, rowGroups[i].num_columns, rowGroups[i].columns);
            }
            rowGroups[i].total_byte_size = *readAs(_this, 1, int64_t);
            rowGroups[i].num_rows = *readAs(_this, 1, int64_t);
            rowGroups[i].num_sorting_columns = *readAs(_this, 1, unsigned short);
            if (rowGroups[i].sorting_columns > 0) {
                rowGroups[i].sorting_columns = malloc(rowGroups[i].num_sorting_columns * sizeof(SortingColumn));
                readSortingColumns(_this, rowGroups[i].num_sorting_columns, rowGroups[i].sorting_columns);
            }
        }
    }
}

static void readColumnChunks(FileMetaBuffer *_this, unsigned short num_columns, ColumnChunk *columns) {
    if (columns) {
        int i;
        for (i = 0; i < num_columns; ++i) {
            readImmutableStrings(_this, 1, &(columns[i].file_path));
            columns[i].file_offset = *readAs(_this, 1, int64_t);
            readColumnMetaDatas(_this, 1, &(columns[i].meta_data));
            columns[i].offset_index_offset = *readAs(_this, 1, int64_t);
            columns[i].offset_index_length = *readAs(_this, 1, int32_t);
            columns[i].column_index_offset = *readAs(_this, 1, int64_t);
            columns[i].column_index_length = *readAs(_this, 1, int32_t);
        }
    }
}

static void readColumnMetaDatas(FileMetaBuffer *_this, unsigned short num_metas, ColumnMetaData *columnMetaDatas) {
    if (columnMetaDatas) {
        int i;
        for (i = 0; i < num_metas; ++i) {
            columnMetaDatas[i].type = *readAs(_this, 1, Type);

            columnMetaDatas[i].num_encodings = *readAs(_this, 1, unsigned short);
            if (columnMetaDatas[i].num_encodings > 0) {
                columnMetaDatas[i].encodings = malloc(columnMetaDatas[i].num_encodings * sizeof(Encoding));
                columnMetaDatas[i].encodings = readAs(_this, columnMetaDatas[i].num_encodings, Encoding);
            }

            columnMetaDatas[i].num_paths = *readAs(_this, 1, unsigned short);
            if (columnMetaDatas[i].num_paths > 0) {
                columnMetaDatas[i].path_in_schema = malloc(columnMetaDatas[i].num_paths * sizeof(immutable_string));
                readImmutableStrings(_this, columnMetaDatas[i].num_paths, columnMetaDatas[i].path_in_schema);
            }
            columnMetaDatas[i].codec = *readAs(_this, 1, CompressionCodec);
            columnMetaDatas[i].num_values = *readAs(_this, 1, int64_t);
            columnMetaDatas[i].total_uncompressed_size = *readAs(_this, 1, int64_t);
            columnMetaDatas[i].total_compressed_size = *readAs(_this, 1, int64_t);

            columnMetaDatas[i].num_kvs = *readAs(_this, 1, int32_t);
            if (columnMetaDatas[i].num_kvs > 0) {
                columnMetaDatas[i].key_value_metadata = malloc(columnMetaDatas[i].num_kvs * sizeof(KeyValue));
                readKeyValues(_this, columnMetaDatas[i].num_kvs, columnMetaDatas[i].key_value_metadata);
            }

            columnMetaDatas[i].data_page_offset = *readAs(_this, 1, int64_t);
            columnMetaDatas[i].index_page_offset = *readAs(_this, 1, int64_t);
            columnMetaDatas[i].dictionary_page_offset = *readAs(_this, 1, int64_t);

            readStatistics(_this, 1, &(columnMetaDatas[i].statistics));

            columnMetaDatas[i].num_stats = *readAs(_this, 1, unsigned short);
            if (columnMetaDatas[i].num_stats > 0) {
                columnMetaDatas[i].encoding_stats = malloc(columnMetaDatas[i].num_stats * sizeof(PageEncodingStats));
                readPageEncodingStats(_this, columnMetaDatas[i].num_stats, columnMetaDatas[i].encoding_stats);
            }
        }
    }
}

static void readKeyValues(FileMetaBuffer *_this, int32_t num_kvs, KeyValue *kvs) {
    if (kvs) {
        int i;
        for (i = 0; i < num_kvs; ++i) {
            readImmutableStrings(_this, 1, &(kvs[i].key));
            readImmutableStrings(_this, 1, &(kvs[i].value));
        }
    }
}

static void readStatistics(FileMetaBuffer *_this, int32_t num_statistics, Statistics *statistics) {
    if (statistics) {
        int i;
        for (i = 0; i < num_statistics; ++i) {
            readImmutableStrings(_this, 1, &(statistics[i].max));
            readImmutableStrings(_this, 1, &(statistics[i].min));
            statistics[i].null_count = *readAs(_this, 1, int64_t);
            statistics[i].distinct_count = *readAs(_this, 1, int64_t);
            readImmutableStrings(_this, 1, &(statistics[i].max_value));
            readImmutableStrings(_this, 1, &(statistics[i].min_value));
        }
    }
}

static void readPageEncodingStats(FileMetaBuffer *_this, unsigned short num_stats, PageEncodingStats *stats) {
    if (stats) {
        int i;
        for (i = 0; i < num_stats; ++i) {
            stats[i].page_type = *readAs(_this, 1, PageType);
            stats[i].encoding = *readAs(_this, 1, Encoding);
            stats[i].count = *readAs(_this, 1, int32_t);
        }
    }
}

static void
readSortingColumns(FileMetaBuffer *_this, unsigned short num_sorting_columns, SortingColumn *sortingColumns) {
    if (sortingColumns && num_sorting_columns > 0) {
        int i;
        for (i = 0; i < num_sorting_columns; ++i) {
            sortingColumns[i].column_idx = *readAs(_this, 1, int32_t);
            sortingColumns[i].descending = *readAs(_this, 1, bool);
            sortingColumns[i].nulls_first = *readAs(_this, 1, bool);
        }
    }
}

static void readImmutableStrings(FileMetaBuffer *_this, int32_t num_str, immutable_string *str) {
    if (str && num_str > 0) {
        int i;
        for (i = 0; i < num_str; ++i) {
            str[i].length = *readAs(_this, 1, unsigned short);
            str[i].str = malloc(str[i].length * sizeof(char));
            str->str = readAs(_this, str[i].length, char);
        }
    }
}
