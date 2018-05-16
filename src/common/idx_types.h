//
// Created by wangpengyu6 on 18-5-11.
//

#ifndef INDEXED_FILE_IDX_TYPES_H
#define INDEXED_FILE_IDX_TYPES_H

#include <stdint.h>
#include <stdio.h>
#include <stdbool.h>
#include "idx_constants.h"

struct Type {
    enum t {
        BOOLEAN = 0,
        INT32 = 1,
        INT64 = 2,
        INT96 = 3,
        FLOAT = 4,
        DOUBLE = 5,
        BYTE_ARRAY = 6,
        FIXED_LEN_BYTE_ARRAY = 7
    } type;
};
struct ConvertedType {
    enum converted_type {
        UTF8 = 0,
        MAP = 1,
        MAP_KEY_VALUE = 2,
        LIST = 3,
        ENUM = 4,
        DECIMAL = 5,
        DATE = 6,
        TIME_MILLIS = 7,
        TIME_MICROS = 8,
        TIMESTAMP_MILLIS = 9,
        TIMESTAMP_MICROS = 10,
        UINT_8 = 11,
        UINT_16 = 12,
        UINT_32 = 13,
        UINT_64 = 14,
        INT_8 = 15,
        INT_16 = 16,
        INT_32 = 17,
        INT_64 = 18,
        JSON = 19,
        BSON = 20,
        INTERVAL = 21
    } type;
};
struct FieldRepetitionType {
    enum repetition_type {
        REQUIRED = 0,
        OPTIONAL = 1,
        REPEATED = 2
    } type;
};
struct Encoding {
    enum encode_type {
        PLAIN = 0,
        PLAIN_DICTIONARY = 2,
        RLE = 3,
        BIT_PACKED = 4,
        DELTA_BINARY_PACKED = 5,
        DELTA_LENGTH_BYTE_ARRAY = 6,
        DELTA_BYTE_ARRAY = 7,
        RLE_DICTIONARY = 8
    } type;
};
struct CompressionCodec {
    enum compression_type {
        UNCOMPRESSED = 0,
        SNAPPY = 1,
        GZIP = 2,
        LZO = 3,
        BROTLI = 4,
        LZ4 = 5,
        ZSTD = 6
    } type;
};
struct PageType {
    enum page_type {
        DATA_PAGE = 0,
        INDEX_PAGE = 1,
        DICTIONARY_PAGE = 2,
        DATA_PAGE_V2 = 3
    } type;
};
struct BoundaryOrder {
    enum boundary_type {
        UNORDERED = 0,
        ASCENDING = 1,
        DESCENDING = 2
    } type;
};

typedef struct Statistics Statistics;

typedef struct StringType StringType;

struct UUIDType;

struct MapType;

struct ListType;

struct EnumType;

struct DateType;

struct NullType;

struct DecimalType;

struct MilliSeconds;

struct MicroSeconds;

struct TimeUnit;

struct TimestampType;

struct TimeType;

struct IntType;

struct JsonType;

struct BsonType;

struct LogicalType;

struct SchemaElement;

struct DataPageHeader;

struct IndexPageHeader;

struct DictionaryPageHeader;

struct DataPageHeaderV2;

struct PageHeader;

struct KeyValue;

struct SortingColumn;

struct PageEncodingStats;

struct ColumnMetaData;

struct ColumnChunk;

struct RowGroup;

struct TypeDefinedOrder;

struct ColumnOrder;

struct PageLocation;

struct OffsetIndex;

struct ColumnIndex;

struct FileMetaData;

//Statistics
struct Statistics {
    const Statistics *This;
    immutable_string max;
    immutable_string min;
    int64_t null_count;
    int64_t distinct_count;
    immutable_string max_value;
    immutable_string min_value;

    bool (*equals)(const Statistics *This, const Statistics *that);

    //将字符串写入到s中
    void (*toString)(const Statistics *This, char *str);

    Statistics *(*free)(Statistics *This);
};

Statistics *initStatistics();

Statistics *initStatistics_(immutable_string max, immutable_string min, int64_t nullCount, int64_t distinctCount,
                            immutable_string maxValue, immutable_string minValue);

//String
struct StringType {
    immutable_string str;

    bool (*equals)(const StringType *This, const StringType *that);

    bool (*toString)(const StringType *This);
};

#endif //INDEXED_FILE_IDX_TYPES_H
