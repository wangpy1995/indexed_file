//
// Created by wangpengyu6 on 18-5-17.
//

#ifndef INDEXED_FILE_INDEX_H
#define INDEXED_FILE_INDEX_H

#include "idx_types.h"

struct PageLocation {
    int64_t offset;
    int32_t compressed_page_size;
    int64_t first_row_index;
};
struct OffsetIndex {
    PageLocation *page_locations;
};
struct ColumnIndex {
    bool *null_pages;
    immutable_string *min_values;
    immutable_string *max_values;
    BoundaryOrder boundary_order;
    int64_t *null_counts;
};

#endif //INDEXED_FILE_INDEX_H
