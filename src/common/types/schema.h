//
// Created by wangpengyu6 on 18-5-17.
//

#ifndef INDEXED_FILE_SCHEMA_H
#define INDEXED_FILE_SCHEMA_H

#include "idx_types.h"
static const String MAGIC = {.length = 4, .str = "IDX1"};

struct StringType {
};

struct UUIDType {
};

struct MapType {};
struct ListType {
};
struct EnumType {
};

struct DateType {
};

struct NullType {
};

struct DecimalType {
    int32_t scale;
    int32_t precision;
};

struct MilliSeconds {
};
struct MicroSeconds {
};
struct TimeUnit {
    MilliSeconds MILLIS;
    MicroSeconds MICROS;
};
struct TimestampType {
    bool isAjustToUTC;
    TimeUnit unit;
};
struct TimeType {
    bool isAjustToUTC;
    TimeUnit unit;
};

struct IntType {
    int8_t bitWidth;
    bool isSigned;
};

struct JsonType {
};
struct BsonType {
};

struct LogicalType {
    StringType STRING;
    MapType MAP;
    ListType LIST;
    EnumType ENUM;
    DecimalType DECIMAL;
    DateType DATE;
    TimeType TIME;
    TimestampType TIMESTAMP;
    IntType INTEGER;
    NullType UNKNOWN;
    JsonType JSON;
    BsonType BSON;
};

struct SchemaElement {
    String name;
    Type type;
    int32_t type_length;
    FieldRepetitionType repetition_type;
    int32_t num_children;
    ConvertedType converted_type;
    int32_t scale;
    int32_t precision;
    int32_t field_id;
    LogicalType logicalType;
}__attribute__ ((packed));

#endif //INDEXED_FILE_SCHEMA_H
