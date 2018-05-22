//
// Created by wangpengyu6 on 18-5-19.
//

#include <stdlib.h>
#include <common/types/meta_data.h>
#include <common/types/schema.h>
#include "file_meta_reader.h"

static void read(struct MetaDataReader *_this, size_t size, void *res);

static void seekTo(struct MetaDataReader *_this, size_t pos);

static void flush(MetaDataReader *_this);

static void close(struct MetaDataReader *_this);

static void readSchemas(MetaDataReader *_this, unsigned short num_schemas, SchemaElement *schemas);

static void readRowGroups(MetaDataReader *_this, unsigned short num_groups, RowGroup *rowGroups);

static void readKeyValues(MetaDataReader *_this, int32_t num_kvs, KeyValue *kvs);

static void readImmutableStrings(MetaDataReader *_this, int32_t num_str, immutable_string *str);

static void readColumnChunks(MetaDataReader *_this, unsigned short num_columns, ColumnChunk *columns);

static void readColumnMetaDatas(MetaDataReader *_this, unsigned short num_metas, ColumnMetaData *columnMetaDatas);

static void readStatistics(MetaDataReader *_this, int32_t num_statistics, Statistics *statistics);

static void readPageEncodingStats(MetaDataReader *_this, unsigned short num_stats, PageEncodingStats *stats);

static void
readSortingColumns(MetaDataReader *_this, unsigned short num_sorting_columns, SortingColumn *sortingColumns);

void readFileMeta(MetaDataReader *_this, FileMetaData *metaData);

MetaDataReader *createMetaDataReader(FILE *fp) {
    if (fp) {
        MetaDataReader *reader = malloc(sizeof(MetaDataReader));
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

static void read(struct MetaDataReader *_this, size_t size, void *res) {
    if (_this && size > 0 && res) {
        fseek(_this->fp, _this->pos, SEEK_SET);
        fread(res, size, 1, _this->fp);
        _this->pos += size;
    }
}

static void seekTo(struct MetaDataReader *_this, size_t pos) {
    if (_this && pos > 0) {
        _this->pos = pos;
    }
}

static void flush(MetaDataReader *_this) {
    if (_this) {
        fflush(_this->fp);
    }
}

static void close(struct MetaDataReader *_this) {
    if (_this) {
        fclose(_this->fp);
        free(_this);
        _this->fp = NULL;
    }
}

void readFileMeta(MetaDataReader *_this, FileMetaData *metaData) {
    if (_this && metaData) {
        //version  int32
        read(_this, sizeof(int32_t), &(metaData->version));

        //schemas
        read(_this, sizeof(unsigned short), &(metaData->num_schemas));
        if (metaData->num_schemas > 0) {
            metaData->schema = malloc(metaData->num_schemas * sizeof(SchemaElement));
            readSchemas(_this, metaData->num_schemas, metaData->schema);
        }
        //numRows int64
        read(_this, sizeof(int64_t), &(metaData->num_rows));

        //rowGroups
        read(_this, sizeof(unsigned short), &(metaData->num_groups));
        if (metaData->num_groups > 0) {
            metaData->row_groups = malloc(metaData->num_schemas * sizeof(RowGroup));
            readRowGroups(_this, metaData->num_groups, metaData->row_groups);
        }

        //kv
        read(_this, sizeof(int32_t), &(metaData->num_kvs));
        if (metaData->num_kvs > 0) {
            metaData->key_value_metadata = malloc(metaData->num_kvs * sizeof(KeyValue));
            readKeyValues(_this, metaData->num_kvs, metaData->key_value_metadata);
        }
        //createBy
        readImmutableStrings(_this, 1, &(metaData->created_by));

        //columnOrders
        read(_this, sizeof(unsigned char), &(metaData->num_column_orders));
        if (metaData->num_column_orders > 0) {
            metaData->column_orders = malloc(metaData->num_column_orders * sizeof(ColumnOrder));
            read(_this, metaData->num_column_orders * sizeof(ColumnOrder), metaData->column_orders);
        }
    }
}

static void readSchemas(MetaDataReader *_this, unsigned short num_schemas, SchemaElement *schemas) {
    if (schemas) {
        int i;
        for (i = 0; i < num_schemas; ++i) {
            //name
            readImmutableStrings(_this, 1, &(schemas[i].name));
            //type
            read(_this, sizeof(Type), &(schemas[i].type));

            //type_length
            read(_this, sizeof(int32_t), &(schemas[i].type_length));

            //repetition_type
            read(_this, sizeof(FieldRepetitionType), &(schemas[i].repetition_type));

            //num_children
            read(_this, sizeof(int32_t), &(schemas[i].num_children));

            //convertedType
            read(_this, sizeof(ConvertedType), &(schemas[i].converted_type));

            //scale
            read(_this, sizeof(int32_t), &(schemas[i].scale));

            //precision
            read(_this, sizeof(int32_t), &(schemas[i].precision));

            //field_id
            read(_this, sizeof(int32_t), &(schemas[i].field_id));

            //logicalType
            read(_this, sizeof(LogicalType), &(schemas[i].logicalType));
        }
    }
}

static void readRowGroups(MetaDataReader *_this, unsigned short num_groups, RowGroup *rowGroups) {
    if (rowGroups) {
        int i;
        for (i = 0; i < num_groups; ++i) {
            read(_this, sizeof(unsigned short), &(rowGroups[i].num_columns));
            if (rowGroups[i].num_columns > 0) {
                rowGroups[i].columns = malloc(rowGroups[i].num_columns * sizeof(ColumnChunk));
                readColumnChunks(_this, rowGroups[i].num_columns, rowGroups[i].columns);
            }
            read(_this, sizeof(int64_t), &(rowGroups[i].total_byte_size));
            read(_this, sizeof(int64_t), &(rowGroups[i].num_rows));
            read(_this, sizeof(int64_t), &(rowGroups[i].num_sorting_columns));
            if (rowGroups[i].sorting_columns > 0) {
                rowGroups[i].sorting_columns = malloc(rowGroups[i].num_sorting_columns * sizeof(SortingColumn));
                readSortingColumns(_this, rowGroups[i].num_sorting_columns, rowGroups[i].sorting_columns);
            }
        }
    }
}

static void readColumnChunks(MetaDataReader *_this, unsigned short num_columns, ColumnChunk *columns) {
    if (columns) {
        int i;
        for (i = 0; i < num_columns; ++i) {
            readImmutableStrings(_this, 1, &(columns[i].file_path));
            read(_this, sizeof(int64_t), &(columns[i].file_offset));
            readColumnMetaDatas(_this, 1, &(columns[i].meta_data));
            read(_this, sizeof(int64_t), &(columns[i].offset_index_offset));
            read(_this, sizeof(int64_t), &(columns[i].offset_index_length));
            read(_this, sizeof(int64_t), &(columns[i].column_index_offset));
            read(_this, sizeof(int64_t), &(columns[i].column_index_length));
        }
    }
}

static void readColumnMetaDatas(MetaDataReader *_this, unsigned short num_metas, ColumnMetaData *columnMetaDatas) {
    if (columnMetaDatas) {
        int i;
        for (i = 0; i < num_metas; ++i) {
            read(_this, sizeof(Type), &(columnMetaDatas[i].type));

            read(_this, sizeof(unsigned short), &(columnMetaDatas[i].num_encodings));
            if (columnMetaDatas[i].num_encodings > 0) {
                columnMetaDatas[i].encodings = malloc(columnMetaDatas[i].num_encodings * sizeof(Encoding));
                read(_this, columnMetaDatas[i].num_encodings * sizeof(Encoding), columnMetaDatas[i].encodings);
            }

            read(_this, sizeof(unsigned short), &(columnMetaDatas[i].num_paths));
            if (columnMetaDatas[i].num_paths > 0) {
                columnMetaDatas[i].path_in_schema = malloc(columnMetaDatas[i].num_paths * sizeof(immutable_string));
                readImmutableStrings(_this, columnMetaDatas[i].num_paths, columnMetaDatas[i].path_in_schema);
            }
            read(_this, sizeof(CompressionCodec), &(columnMetaDatas[i].codec));
            read(_this, sizeof(int64_t), &(columnMetaDatas[i].num_values));
            read(_this, sizeof(int64_t), &(columnMetaDatas[i].total_uncompressed_size));
            read(_this, sizeof(int64_t), &(columnMetaDatas[i].total_compressed_size));

            read(_this, sizeof(int32_t), &(columnMetaDatas[i].num_kvs));
            if (columnMetaDatas[i].num_kvs > 0) {
                columnMetaDatas[i].key_value_metadata = malloc(columnMetaDatas[i].num_kvs * sizeof(KeyValue));
                readKeyValues(_this, columnMetaDatas[i].num_kvs, columnMetaDatas[i].key_value_metadata);
            }

            read(_this, sizeof(int64_t), &(columnMetaDatas[i].data_page_offset));
            read(_this, sizeof(int64_t), &(columnMetaDatas[i].index_page_offset));
            read(_this, sizeof(int64_t), &(columnMetaDatas[i].dictionary_page_offset));

            readStatistics(_this, 1, &(columnMetaDatas[i].statistics));

            read(_this, sizeof(unsigned short), &(columnMetaDatas[i].num_stats));
            if (columnMetaDatas[i].num_stats > 0) {
                columnMetaDatas[i].encoding_stats = malloc(columnMetaDatas[i].num_stats * sizeof(PageEncodingStats));
                readPageEncodingStats(_this, columnMetaDatas[i].num_stats, columnMetaDatas[i].encoding_stats);
            }
        }
    }
}

static void readKeyValues(MetaDataReader *_this, int32_t num_kvs, KeyValue *kvs) {
    if (kvs) {
        int i;
        for (i = 0; i < num_kvs; ++i) {
            readImmutableStrings(_this, 1, &(kvs[i].key));
            readImmutableStrings(_this, 1, &(kvs[i].value));
        }
    }
}

static void readStatistics(MetaDataReader *_this, int32_t num_statistics, Statistics *statistics) {
    if (statistics) {
        int i;
        for (i = 0; i < num_statistics; ++i) {
            readImmutableStrings(_this, 1, &(statistics[i].max));
            readImmutableStrings(_this, 1, &(statistics[i].min));
            read(_this, sizeof(int64_t), &(statistics[i].null_count));
            read(_this, sizeof(int64_t), &(statistics[i].distinct_count));
            readImmutableStrings(_this, 1, &(statistics[i].max_value));
            readImmutableStrings(_this, 1, &(statistics[i].min_value));
        }
    }
}

static void readPageEncodingStats(MetaDataReader *_this, unsigned short num_stats, PageEncodingStats *stats) {
    if (stats) {
        int i;
        for (i = 0; i < num_stats; ++i) {
            read(_this, sizeof(PageType), &(stats[i].page_type));
            read(_this, sizeof(Encoding), &(stats[i].encoding));
            read(_this, sizeof(int32_t), &(stats[i].count));
        }
    }
}

static void
readSortingColumns(MetaDataReader *_this, unsigned short num_sorting_columns, SortingColumn *sortingColumns) {
    if (sortingColumns && num_sorting_columns > 0) {
        int i;
        for (i = 0; i < num_sorting_columns; ++i) {
            read(_this, sizeof(int32_t), &(sortingColumns[i].column_idx));
            read(_this, sizeof(bool), &(sortingColumns[i].descending));
            read(_this, sizeof(bool), &(sortingColumns[i].nulls_first));
        }
    }
}

static void readImmutableStrings(MetaDataReader *_this, int32_t num_str, immutable_string *str) {
    if (str && num_str > 0) {
        int i;
        for (i = 0; i < num_str; ++i) {
            read(_this, sizeof(unsigned short), &(str[i].length));
            str[i].str = malloc(str[i].length * sizeof(char));
            read(_this, str[i].length * sizeof(char), (void *) str->str);
        }
    }
}