//
// Created by wangpengyu6 on 18-5-11.
//

#ifndef INDEXED_FILE_IDX_TYPES_H
#define INDEXED_FILE_IDX_TYPES_H

#include <stdint.h>
#include <stdio.h>
#include <stdbool.h>
#include "idx_constants.h"

typedef enum Type {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    INT96 = 3,
    FLOAT = 4,
    DOUBLE = 5,
    BYTE_ARRAY = 6,
    FIXED_LEN_BYTE_ARRAY = 7
} Type;

typedef enum ConvertedType {
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
} ConvertedType;
typedef enum FieldRepetitionType {
    REQUIRED = 0,
    OPTIONAL = 1,
    REPEATED = 2
} FieldRepetitionType;
typedef enum Encoding {
    PLAIN = 0,
    PLAIN_DICTIONARY = 2,
    RLE = 3,
    BIT_PACKED = 4,
    DELTA_BINARY_PACKED = 5,
    DELTA_LENGTH_BYTE_ARRAY = 6,
    DELTA_BYTE_ARRAY = 7,
    RLE_DICTIONARY = 8
} Encoding;
typedef enum CompressionCodec {
    UNCOMPRESSED = 0,
    SNAPPY = 1,
    GZIP = 2,
    LZO = 3,
    BROTLI = 4,
    LZ4 = 5,
    ZSTD = 6
} CompressionCodec;
typedef enum PageType {
    DATA_PAGE = 0,
    INDEX_PAGE = 1,
    DICTIONARY_PAGE = 2,
    DATA_PAGE_V2 = 3
} PageType;
typedef enum BoundaryOrder {
    UNORDERED = 0,
    ASCENDING = 1,
    DESCENDING = 2
} BoundaryOrder;


////////////////////////////////////////////////////////////
//   Logical Types
////////////////////////////////////////////////////////////

typedef struct Statistics Statistics;
typedef struct StringType StringType;
typedef struct UUIDType UUIDType;
typedef struct MapType MapType;
typedef struct ListType ListType;
typedef struct EnumType EnumType;
typedef struct DateType DateType;
typedef struct NullType NullType;
typedef struct DecimalType DecimalType;
typedef struct MilliSeconds MilliSeconds;
typedef struct MicroSeconds MicroSeconds;
typedef struct TimeUnit TimeUnit;
typedef struct TimestampType TimestampType;
typedef struct TimeType TimeType;
typedef struct IntType IntType;
typedef struct JsonType JsonType;
typedef struct BsonType BsonType;
typedef struct LogicalType LogicalType;
typedef struct SchemaElement SchemaElement;
typedef struct DataPageHeader DataPageHeader;
typedef struct IndexPageHeader IndexPageHeader;
typedef struct DictionaryPageHeader DictionaryPageHeader;
typedef struct DataPageHeaderV2 DataPageHeaderV2;
typedef struct PageHeader PageHeader;
typedef struct KeyValue KeyValue;
typedef struct SortingColumn SortingColumn;
typedef struct PageEncodingStats PageEncodingStats;
typedef struct ColumnMetaData ColumnMetaData;
typedef struct ColumnChunk ColumnChunk;
typedef struct RowGroup RowGroup;
typedef struct TypeDefinedOrder TypeDefinedOrder;
typedef struct ColumnOrder ColumnOrder;
typedef struct PageLocation PageLocation;
typedef struct OffsetIndex OffsetIndex;
typedef struct ColumnIndex ColumnIndex;
typedef struct FileMetaData FileMetaData;

#endif //INDEXED_FILE_IDX_TYPES_H
