//
// Created by wangpengyu6 on 18-5-11.
//

#ifndef INDEXED_FILE_TEST_IDX_TYPES_H
#define INDEXED_FILE_TEST_IDX_TYPES_H

#include <stdint.h>
#include <mhash.h>

struct Type {
    enum type {
        BOOLEAN = 0,
        INT32 = 1,
        INT64 = 2,
        INT96 = 3,
        FLOAT = 4,
        DOUBLE = 5,
        BYTE_ARRAY = 6,
        FIXED_LEN_BYTE_ARRAY = 7
    };
};
struct ConvertedType {
    enum type {
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
    };
};
struct FieldRepetitionType {
    enum type {
        REQUIRED = 0,
        OPTIONAL = 1,
        REPEATED = 2
    };
};
struct Encoding {
    enum type {
        PLAIN = 0,
        PLAIN_DICTIONARY = 2,
        RLE = 3,
        BIT_PACKED = 4,
        DELTA_BINARY_PACKED = 5,
        DELTA_LENGTH_BYTE_ARRAY = 6,
        DELTA_BYTE_ARRAY = 7,
        RLE_DICTIONARY = 8
    };
};
struct CompressionCodec {
    enum type {
        UNCOMPRESSED = 0,
        SNAPPY = 1,
        GZIP = 2,
        LZO = 3,
        BROTLI = 4,
        LZ4 = 5,
        ZSTD = 6
    };
};
struct PageType {
    enum type {
        DATA_PAGE = 0,
        INDEX_PAGE = 1,
        DICTIONARY_PAGE = 2,
        DATA_PAGE_V2 = 3
    };
};
struct BoundaryOrder {
    enum type {
        UNORDERED = 0,
        ASCENDING = 1,
        DESCENDING = 2
    };
};

typedef struct Statistics Statistics;

struct StringType;

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

struct Statistics {
    const Statistics *This;

    char *max;
    char *min;
    int64_t null_count;
    int64_t distinct_count;
    char *max_value;
    char *min_value;

    bool (*equals)(const Statistics *This, const Statistics *that);
};

bool statistics_equals(const Statistics *This, const Statistics *that);

Statistics* initStatistics();

#endif //INDEXED_FILE_TEST_IDX_TYPES_H
