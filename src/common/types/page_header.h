//
// Created by wangpengyu6 on 18-5-17.
//

#ifndef INDEXED_FILE_DATA_PAGE_HEADER_H
#define INDEXED_FILE_DATA_PAGE_HEADER_H

#include "idx_types.h"

struct DataPageHeader {
    int32_t num_values;
    Encoding encoding;
    Encoding definition_level_encoding;
    Encoding repetition_level_encoding;
    Statistics statistics;
};
struct IndexPageHeader {
};
struct DictionaryPageHeader {
    int32_t num_values;
    Encoding encoding;
    bool is_sorted;
};
struct DataPageHeaderV2 {
    int32_t num_values;
    int32_t num_nulls;
    int32_t num_rows;
    Encoding encoding;
    //根据长度使用位图节省空间
    int32_t definition_levels_byte_length;
    int32_t repetition_levels_byte_length;
    bool is_compressed;
    Statistics statistics;
};
struct PageHeader {
    PageType type;
    int32_t uncompressed_page_size;
    int32_t compressed_page_size;
    int32_t crc;
    DataPageHeader data_page_header;
    IndexPageHeader index_page_header;
    DictionaryPageHeader dictionary_page_header;
    DataPageHeaderV2 data_page_header_v2;
};
#endif //INDEXED_FILE_DATA_PAGE_HEADER_H
