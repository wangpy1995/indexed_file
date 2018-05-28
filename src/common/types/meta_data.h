//
// Created by wangpengyu6 on 18-5-17.
//

#ifndef INDEXED_FILE_META_DATA_H
#define INDEXED_FILE_META_DATA_H

#include "idx_types.h"
#include "statistics.h"
#include "schema.h"

struct SortingColumn {
    int32_t column_idx;
    bool descending;
    bool nulls_first;
} __attribute__ ((packed));
struct PageEncodingStats {
    PageType page_type;
    Encoding encoding;
    int32_t count;
}__attribute__ ((packed));
/*單列相關信息*/
struct ColumnMetaData {
    /*字段類型*/
    Type type;
    /*數據編碼信息*/
    unsigned short encoding_len;
    Encoding *encodings;

    unsigned short path_len;
    String *path_in_schema;

    /*數據壓縮類型*/
    CompressionCodec codec;

    /*該列值的數量*/
    int64_t num_values;
    /*編碼與壓縮前的數據字節數*/
    int64_t total_uncompressed_size;
    /*壓縮後字節數*/
    int64_t total_compressed_size;
    /*鍵值對*/
    int32_t kv_len;
    KeyValue *key_value_metadata;
    /*該列數據在 data page 的偏移量*/
    int64_t data_page_offset;
    /*該列在 index page 中的偏移量*/
    int64_t index_page_offset;
    /*該列在 dictionary page 中的偏移量*/
    int64_t dictionary_page_offset;
    /*列簡單屬性*/
    Statistics statistics;

    unsigned short stat_len;
    PageEncodingStats *encoding_stats;
}__attribute__ ((packed));

struct ColumnChunk {
    String file_path;
    int64_t file_offset;
    ColumnMetaData meta_data;
    int64_t offset_index_offset;
    int32_t offset_index_length;
    int64_t column_index_offset;
    int32_t column_index_length;
}__attribute__ ((packed));

/*保存多組列信息*/
struct RowGroup {
    /*列信息*/
    unsigned short chunk_len;
    ColumnChunk *columns;
    /*列所佔字節數*/
    int64_t total_byte_size;
    /*列記錄總數*/
    int64_t num_rows;
    /*列排序規則*/
    unsigned short sorting_columns_len;
    SortingColumn *sorting_columns;
}__attribute__ ((packed));

/*自定義鍵值對信息*/
struct KeyValue {
    String key;
    String value;
}__attribute__ ((packed));

struct TypeDefinedOrder {
}__attribute__ ((packed));
struct ColumnOrder {
    TypeDefinedOrder TYPE_ORDER;
}__attribute__ ((packed));

//最大支持65535个字段  65535个rowGroup  INT_MAX个kv
struct FileMetaData {
    /*版本信息*/
    int32_t version;

    /*schema element 信息字節數,可用於讀取文件時跳過*/
    unsigned short schema_len;
    SchemaElement *schema;

    /*文件保存記錄總數*/
    int64_t num_rows;

    //row group 信息字節數,可用於讀取文件時跳過
    unsigned short group_len;
    RowGroup *row_groups;

    /*key value 信息字節數,可用於讀取文件時跳過*/
    int32_t kv_len;
    KeyValue *key_value_metadata;

    /*文件創建信息*/
    String created_by;

    /*內部排序相關信息*/
    unsigned char order_len;
    ColumnOrder *column_orders;
}__attribute__ ((packed));


static void freeFileMetaData(FileMetaData **metaPointer) {
    if (metaPointer && *metaPointer) {
        const FileMetaData *meta = *metaPointer;
        int i;
        if (meta->schema) {
            if (meta->schema->name.str) {
                free(meta->schema->name.str);
                meta->schema->name.str = NULL;
            }
            free(meta->schema);
        }
        if (meta->row_groups) {
            for (i = 0; i < meta->group_len; ++i) {
                if (meta->row_groups->columns) {
                    free(meta->row_groups->columns);
                    meta->row_groups->columns = NULL;
                }
                if (meta->row_groups->sorting_columns) {
                    free(meta->row_groups->sorting_columns);
                    meta->row_groups->sorting_columns = NULL;
                }
            }
            free(meta->row_groups);
        }
        if (meta->column_orders) {
            free(meta->column_orders);
        }
        if (meta->key_value_metadata) {
            for (i = 0; i < meta->kv_len; ++i) {
                if (meta->key_value_metadata->key.str) {
                    free(meta->key_value_metadata->key.str);
                    meta->key_value_metadata->key.str = NULL;
                }
                if (meta->key_value_metadata->value.str) {
                    free(meta->key_value_metadata->value.str);
                    meta->key_value_metadata->value.str = NULL;
                }
            }
            free(meta->key_value_metadata);
        }

        if (meta->created_by.str) {
            free(meta->created_by.str);
        }
        *metaPointer = NULL;
    }
}

#endif //INDEXED_FILE_META_DATA_H
