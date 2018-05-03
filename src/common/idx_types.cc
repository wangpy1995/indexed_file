//
// Created by wangpengyu6 on 18-5-3.
//

#include "idx_types.h"

namespace idx {
    std::string CompressionToString(Compression::type t) {
        switch (t) {
            case Compression::UNCOMPRESSED:
                return "uncompressed";
            case Compression::SNAPPY:
                return "SNAPPY";
            case Compression::GZIP:
                return "GZIP";
            case Compression::LZO:
                return "LZO";
            case Compression::BROTLI:
                return "BROTLI";
            case Compression::LZ4:
                return "LZ4";
            case Compression::ZSTD:
                return "ZSTD";
            default:
                return "UNKNOWN";
        }
    }

    std::string EncodingToString(Encoding::type t) {
        switch (t) {
            case Encoding::PLAIN:
                return "PLAIN";
            case Encoding::PLAIN_DICTIONARY:
                return "PLAIN_DICTIONARY";
            case Encoding::RLE:
                return "RLE";
            case Encoding::BIT_PACKED:
                return "BIT_PACKED";
            case Encoding::DELTA_BINARY_PACKED :
                return "DELTA_BINARY_PACKED";
            case Encoding::DELTA_LENGTH_BYTE_ARRAY :
                return "DELTA_LENGTH_BYTE_ARRAY";
            case Encoding::DELTA_BYTE_ARRAY :
                return "DELTA_BYTE_ARRAY";
            case Encoding::RLE_DICTIONARY :
                return "RLE_DICTIONARY";
            default:
                return "UNKNOWN";
        }
    }


}
