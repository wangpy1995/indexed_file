//
// Created by wangpengyu6 on 18-5-2.
//

#ifndef INDEXED_FILE_IDX_TYPES_H
#define INDEXED_FILE_IDX_TYPES_H

#include <cstdint>
#include <cstring>
#include <algorithm>
#include <sstream>
#include <iterator>
#include "util/visibility.h"

namespace idx {
    namespace format {
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

// Mirrors idx::ConvertedType
        struct LogicalType {
            enum type {
                NONE,
                UTF8,
                MAP,
                MAP_KEY_VALUE,
                LIST,
                ENUM,
                DECIMAL,
                DATE,
                TIME_MILLIS,
                TIME_MICROS,
                TIMESTAMP_MILLIS,
                TIMESTAMP_MICROS,
                UINT_8,
                UINT_16,
                UINT_32,
                UINT_64,
                INT_8,
                INT_16,
                INT_32,
                INT_64,
                JSON,
                BSON,
                INTERVAL,
                NA = 25
            };
        };

// Mirrors idx::FieldRepetitionType
        struct Repetition {
            enum type {
                REQUIRED = 0, OPTIONAL = 1, REPEATED = 2
            };
        };

// Data encodings. Mirrors idx::Encoding
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

// Compression, mirrors idx::CompressionCodec
        struct Compression {
            enum type {
                UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4, ZSTD
            };
        };

// idx::PageType
        struct PageType {
            enum type {
                DATA_PAGE, INDEX_PAGE, DICTIONARY_PAGE, DATA_PAGE_V2
            };
        };

        struct SortOrder {
            enum type {
                SIGNED, UNSIGNED, UNKNOWN
            };
        };

        class ColumnOrder {
        public:
            enum type {
                UNDEFINED, TYPE_DEFINED_ORDER
            };

            explicit
            ColumnOrder(ColumnOrder::type
                        column_order) : column_order_(column_order) {}

            // Default to Type Defined Order
            ColumnOrder() : column_order_(type::TYPE_DEFINED_ORDER) {}

            ColumnOrder::type get_order() { return column_order_; }

            static ColumnOrder undefined_;
            static ColumnOrder type_defined_;

        private:
            ColumnOrder::type column_order_;
        };

        struct ByteArray {
            ByteArray() : len(0), ptr(nullptr) {}

            ByteArray(uint32_t len, const uint8_t *ptr) : len(len), ptr(ptr) {}

            uint32_t len;
            const uint8_t *ptr;
        };

        inline bool operator==(const ByteArray &left, const ByteArray &right) {
            return left.len == right.len && 0 ==
                                            std::memcmp(left
                                                                .ptr, right.ptr, left.len);
        }

        inline bool operator!=(const ByteArray &left, const ByteArray &right) {
            return !(left == right);
        }

//
        struct FixedLengthByteArray {
            FixedLengthByteArray() : ptr(nullptr) {}

            FixedLengthByteArray(uint8_t *ptr) : ptr(ptr) {}

            uint8_t *ptr;
        };

        using FLBA = FixedLengthByteArray;

//TODO Int96
        __attribute__(aligned(1)) struct Int96 {
            int32_t values[3];
        };

        inline bool operator==(const Int96 &left, const Int96 &right) {
            return std::equal(left.values, left.values + 3, right.values);
        }

        inline bool operator!=(const Int96 &left, const Int96 &right) {
            return !(left == right);
        }

        static inline std::string ByteArrayToString(const ByteArray &a) {
            return std::string((char *) a.ptr, a.len);
        };

        static inline std::string Int96ToString(const Int96 &a) {
            std::ostringstream result;
            std::copy(a.values, a.values + 3, std::ostream_iterator<uint32_t>(result, " "));
            return result.str();
        }

        static inline std::string FixedLengthByteArrayToString(const FixedLengthByteArray &a, int len) {
            std::ostringstream result;
            std::copy(a.ptr, a.ptr + len, std::ostream_iterator<uint32_t>(result, " "));
            return result.str();
        }


        template<Type::type TYPE>
        struct type_traits {
        };

        template<>
        struct type_traits<Type::BOOLEAN> {
            using value_type=bool;
            static constexpr int value_byte_size = 1;
            static constexpr const char *printf_code = "d";
        };

        struct type_traits<Type::INT32> {
            using value_type = int32_t;
            static constexpr int value_byte_size = 4;
            static constexpr const char *printf_code = "d";
        };

        struct type_traits<Type::INT64> {
            using value_type  =  int64_t;
            static constexpr int value_byte_size = 8;
            static constexpr const char *printf_code = "ld";
        };

        struct type_traits<Type::INT96> {
            using value_type = Int96;
            static constexpr int value_byte_size = 12;
            static constexpr const char *printf_code = "s";
        };

        struct type_traits<Type::FLOAT> {
            using value_type=float;
            static constexpr int value_byte_size = 4;
            static constexpr const char *printf_code = "f";
        };

        struct type_traits<Type::DOUBLE> {
            using value_type = double;
            static constexpr int value_byte_size = 8;
            static constexpr const char *printf_code = "lf";
        };

        struct type_traits<Type::BYTE_ARRAY> {
            using value_type=ByteArray;
            static constexpr int value_byte_size = sizeof(ByteArray);
            static constexpr const char *printf_code = "s";
        };

        struct type_traits<Type::FIXED_LEN_BYTE_ARRAY> {
            using value_type=FixedLengthByteArray;
            static constexpr int value_byte_size = sizeof(FixedLengthByteArray);
            static constexpr const char *printf_code = "s";
        };


        template<Type::type TYPE>
        struct DataType {
            using c_type = typename type_traits<TYPE>::value_type;
            static constexpr Type::type type_num = TYPE;
        };

        using BooleanType=DataType<Type::BOOLEAN>;
        using Int32Type = DataType<Type::INT32>;
        using Int64Type = DataType<Type::INT64>;
        using Int96Type = DataType<Type::INT96>;
        using FloatType = DataType<Type::FLOAT>;
        using DoubleType = DataType<Type::DOUBLE>;
        using ByteArrayType = DataType<Type::BYTE_ARRAY>;
        using FLBAType = DataType<Type::FIXED_LEN_BYTE_ARRAY>;

        template<typename Type>
        inline std::string format_fwf(int width) {
            std::stringstream ss;
            ss << "%-" << width << type_traits<Type::type_num>::printf_code;
            return ss.str();
        }

        INDEXED_EXPORT std::string CompressionToString(Compression::type t);

        INDEXED_EXPORT std::string EncodingToString(Encoding::type t);

        INDEXED_EXPORT std::string LogicalTypeToString(LogicalType::type t);

        INDEXED_EXPORT std::string TypeToString(Type::type t);

        INDEXED_EXPORT std::string FormatStatValue(Type::type idx_type, const char *val);

        INDEXED_EXPORT int GetTypeByteSize(Type::type t);

        INDEXED_EXPORT SortOrder::type DefaultForSortOrder(Type::type primitive);

        INDEXED_EXPORT SortOrder::type GetSortOrder(LogicalType::type converted, Type::type primitive);

        typedef struct _Statistics__isset {
            _Statistics__isset() : max(false), min(false), null_count(false), distinct_count(false), max_value(false),
                                   min_value(false) {}

            bool max :1;
            bool min :1;
            bool null_count :1;
            bool distinct_count :1;
            bool max_value :1;
            bool min_value :1;
        } _Statistics__isset;

        class Statistics;

        class StringType;

        class UUIDType;

        class MapType;

        class ListType;

        class EnumType;

        class DateType;

        class NullType;

        class DecimalType;

        class MilliSeconds;

        class MicroSeconds;

        class TimeUnit;

        class TimestampType;

        class TimeType;

        class IntType;

        class JsonType;

        class BsonType;

        class ConvertedType;

        class SchemaElement;

        class DataPageHeader;

        class IndexPageHeader;

        class DictionaryPageHeader;

        class DataPageHeaderV2;

        class PageHeader;

        class KeyValue;

        class SortingColumn;

        class PageEncodingStats;

        class ColumnMetaData;

        class ColumnChunk;

        class RowGroup;

        class TypeDefinedOrder;

        class BoundaryOrder;

        class PageLocation;

        class OffsetIndex;

        class ColumnIndex;

        class FileMetaData;

        class Statistics {
        public:
            Statistics(const Statistics &);

            Statistics &operator=(const Statistics &);

            Statistics() : max(), min(), null_count(0), distinct_count(0), max_value(), min_value() {}

            virtual ~Statistics() throw();

            std::string max;
            std::string min;
            int64_t null_count;
            int64_t distinct_count;
            std::string max_value;
            std::string min_value;

            _Statistics__isset __isset;

            const std::string &getMax() const {
                return max;
            }

            void setMax(const std::string &max) {
                Statistics::max = max;
            }

            const std::string &getMin() const {
                return min;
            }

            void setMin(const std::string &min) {
                Statistics::min = min;
            }

            int64_t getNull_count() const {
                return null_count;
            }

            void setNull_count(int64_t null_count) {
                Statistics::null_count = null_count;
            }

            int64_t getDistinct_count() const {
                return distinct_count;
            }

            void setDistinct_count(int64_t distinct_count) {
                Statistics::distinct_count = distinct_count;
            }

            const std::string &getMax_value() const {
                return max_value;
            }

            void setMax_value(const std::string &max_value) {
                Statistics::max_value = max_value;
            }

            const std::string &getMin_value() const {
                return min_value;
            }

            void setMin_value(const std::string &min_value) {
                Statistics::min_value = min_value;
            }

            bool operator==(const Statistics &rhs) const {
                if (__isset.max != rhs.__isset.max)
                    return false;
                else if (__isset.max && !(max == rhs.max))
                    return false;
                if (__isset.min != rhs.__isset.min)
                    return false;
                else if (__isset.min && !(min == rhs.min))
                    return false;
                if (__isset.null_count != rhs.__isset.null_count)
                    return false;
                else if (__isset.null_count && !(null_count == rhs.null_count))
                    return false;
                if (__isset.distinct_count != rhs.__isset.distinct_count)
                    return false;
                else if (__isset.distinct_count && !(distinct_count == rhs.distinct_count))
                    return false;
                if (__isset.max_value != rhs.__isset.max_value)
                    return false;
                else if (__isset.max_value && !(max_value == rhs.max_value))
                    return false;
                if (__isset.min_value != rhs.__isset.min_value)
                    return false;
                else if (__isset.min_value && !(min_value == rhs.min_value))
                    return false;
                return true;
            }

            bool operator!=(const Statistics &rhs) const {
                return !(*this == rhs);
            }

        };
    }
}//namespace idx
#endif //INDEXED_FILE_IDX_TYPES_H
