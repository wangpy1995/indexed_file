//
// Created by wangpengyu6 on 18-5-17.
//

#ifndef INDEXED_FILE_META_DATA_H
#define INDEXED_FILE_META_DATA_H

#include "idx_types.h"
#include "statistics.h"

struct SortingColumn {
    int32_t column_idx;
    bool descending;
    bool nulls_first;
};
struct PageEncodingStats {
    PageType page_type;
    Encoding encoding;
    int32_t count;
};
struct ColumnMetaData {
    Type type;
    unsigned short num_encodings;
    Encoding *encodings;
    unsigned short num_paths;
    immutable_string *path_in_schema;
    CompressionCodec codec;
    int64_t num_values;
    int64_t total_uncompressed_size;
    int64_t total_compressed_size;
    int32_t num_kvs;
    KeyValue *key_value_metadata;
    int64_t data_page_offset;
    int64_t index_page_offset;
    int64_t dictionary_page_offset;
    Statistics statistics;
    unsigned short num_stats;
    PageEncodingStats *encoding_stats;
};
struct ColumnChunk {
    immutable_string file_path;
    int64_t file_offset;
    ColumnMetaData meta_data;
    int64_t offset_index_offset;
    int32_t offset_index_length;
    int64_t column_index_offset;
    int32_t column_index_length;
};

struct RowGroup {
    unsigned short num_columns;
    ColumnChunk *columns;
    int64_t total_byte_size;
    int64_t num_rows;
    unsigned short num_sorting_columns;
    SortingColumn *sorting_columns;
};
struct KeyValue {
    immutable_string key;
    immutable_string value;
};
struct TypeDefinedOrder {
};
struct ColumnOrder {
    TypeDefinedOrder TYPE_ORDER;
};

//最大支持65535个字段  65535个rowGroup  INT_MAX个kv
struct FileMetaData {
    const int32_t version;
    const unsigned short num_schemas;
    SchemaElement *schema;
    int64_t num_rows;
    const unsigned short num_groups;
    RowGroup *row_groups;
    const int32_t num_kvs;
    KeyValue *key_value_metadata;
    immutable_string created_by;
    const unsigned char num_column_orders;
    ColumnOrder *column_orders;
};
#endif //INDEXED_FILE_META_DATA_H
