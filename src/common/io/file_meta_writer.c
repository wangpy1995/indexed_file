//
// Created by wangpengyu6 on 18-5-19.
//

#include <stdlib.h>
#include <common/types/meta_data.h>
#include <common/types/schema.h>
#include "file_meta_writer.h"
#include <stdbool.h>

static void write(MetaDataWriter *_this, size_t size, const void *data);

static void seekTo(MetaDataWriter *_this, size_t pos);

static void flush(MetaDataWriter *_this);

static void close(MetaDataWriter *_this);

static void writeFileMeta(MetaDataWriter *_this, FileMetaData metaData);

static void writeSchemas(MetaDataWriter *_this, unsigned short num_schemas, SchemaElement *schemas);

static void writeRowGroups(MetaDataWriter *_this, unsigned short num_groups, RowGroup *rowGroups);

static void writeKeyValues(MetaDataWriter *_this, int32_t num_kvs, KeyValue *kvs);

static void writeImmutableStrings(MetaDataWriter *_this, int32_t num_str, const String *strings);

static void writeColumnMetaDatas(MetaDataWriter *_this, unsigned short num_metas, ColumnMetaData *columnMetaDatas);

static void writeColumnChunks(MetaDataWriter *_this, unsigned short num_columns, ColumnChunk *columns);

static void
writeSortingColumns(MetaDataWriter *_this, unsigned short num_sorting_columns, SortingColumn *sortingColumns);

static void writeStatistics(MetaDataWriter *_this, int32_t num_statistics, Statistics *statistics);

static void
writePageEncodingStats(MetaDataWriter *_this, unsigned short num_stats, PageEncodingStats *encoding_stats);

MetaDataWriter *createMetaDataWriter(FILE *fp) {
    if (fp) {
        MetaDataWriter *writer = malloc(sizeof(MetaDataWriter));
        writer->fp = fp;
        writer->pos = 0;
        writer->writeFileMeta = writeFileMeta;
        writer->write = write;
        writer->seekTo = seekTo;
        writer->flush = flush;
        writer->close = close;
        return writer;
    }
    return NULL;
}

static inline void write(MetaDataWriter *_this, size_t size, const void *data) {
    if (_this && size > 0 && data) {
        fseek(_this->fp, _this->pos, SEEK_SET);
        fwrite(data, size, 1, _this->fp);
        _this->pos += size;
    }
}

static inline void seekTo(MetaDataWriter *_this, size_t pos) {
    if (_this) {
        _this->pos = pos;
    }
}

static inline void flush(MetaDataWriter *_this) {
    if (_this) {
        fflush(_this->fp);
    }
}

static inline void close(MetaDataWriter *_this) {
    if (_this) {
        fclose(_this->fp);
        free(_this);
        _this->fp = NULL;
    }
}

//TODO 更改为使用thrift方式
static void writeFileMeta(MetaDataWriter *_this, const FileMetaData metaData) {
    if (_this) {
        /*version  int32*/
        write(_this, sizeof(int32_t), &(metaData.version));

        int32_t len[3];
        /*schema_len  int16  节省空间以及強制轉換  需要单字節對齊*/
        size_t start = _this->pos;
        write(_this, sizeof(unsigned short), &(metaData.schema_len));
//        write(_this, metaData.schema_len, metaData.schema);
        writeSchemas(_this, metaData.schema_len, metaData.schema);
        len[0] = _this->pos - start;

        /*numRows int64*/
        write(_this, sizeof(int64_t), &(metaData.num_rows));
        /*rowGroups*/
        start = _this->pos;
        write(_this, sizeof(unsigned short), &(metaData.group_len));
        writeRowGroups(_this, metaData.group_len, metaData.row_groups);
        len[1] = _this->pos - start;

        /*kv*/
        start = _this->pos;
        write(_this, sizeof(int32_t), &(metaData.kv_len));
//        write(_this, (size_t) metaData.kv_len, metaData.key_value_metadata);
        writeKeyValues(_this, metaData.kv_len, metaData.key_value_metadata);
        len[2] = _this->pos - start;

        /*createBy*/
        writeImmutableStrings(_this, 1, &(metaData.created_by));

        /*columnOrders*/
        write(_this, sizeof(unsigned char), &(metaData.order_len));
        write(_this, metaData.order_len, metaData.column_orders);

        //add schema_size | group_size | kv_size  to tail
        write(_this, sizeof(int32_t)*3, len);
    }
}

static void writeSchemas(MetaDataWriter *_this, unsigned short num_schemas, SchemaElement *schemas) {
    int i;
    for (i = 0; i < num_schemas; ++i) {
        SchemaElement schema = schemas[i];
        //name
        write(_this, sizeof(unsigned short), &(schema.name.length));
        write(_this, (schemas[i]).name.length * sizeof(char), (void *) (schema.name.str));

        //type
        write(_this, sizeof(Type), &(schema.type));

        //type_length
        write(_this, sizeof(int32_t), &(schema.type_length));

        //repetition_type
        write(_this, sizeof(FieldRepetitionType), &(schema.repetition_type));

        //num_children
        write(_this, sizeof(int32_t), &(schema.num_children));

        //convertedType
        write(_this, sizeof(ConvertedType), &(schema.converted_type));

        //scale
        write(_this, sizeof(int32_t), &(schema.scale));

        //precision
        write(_this, sizeof(int32_t), &(schema.precision));

        //field_id
        write(_this, sizeof(int32_t), &(schema.field_id));

        //logicalType
        write(_this, sizeof(LogicalType), &(schema.logicalType));
    }
}

static void writeRowGroups(MetaDataWriter *_this, unsigned short num_groups, RowGroup *rowGroups) {
    int i;
    for (i = 0; i < num_groups; ++i) {
        RowGroup rowGroup = rowGroups[i];

        write(_this, sizeof(unsigned short), &(rowGroup.chunk_len));
        writeColumnChunks(_this, rowGroup.chunk_len, rowGroup.columns);
        write(_this, sizeof(int64_t), &(rowGroup.total_byte_size));
        write(_this, sizeof(int64_t), &(rowGroup.num_rows));
        write(_this, sizeof(int64_t), &(rowGroup.sorting_columns_len));
        write(_this, sizeof(int64_t), &(rowGroup.sorting_columns));
        writeSortingColumns(_this, rowGroup.sorting_columns_len, rowGroup.sorting_columns);
    }
}

static void writeColumnChunks(MetaDataWriter *_this, unsigned short num_columns, ColumnChunk *columns) {
    int i;
    for (i = 0; i < num_columns; ++i) {
        ColumnChunk column = columns[i];
        writeImmutableStrings(_this, 1, &(column.file_path));
        write(_this, sizeof(int64_t), &(column.file_offset));
        writeColumnMetaDatas(_this, 1, &(column.meta_data));
        write(_this, sizeof(int64_t), &(column.offset_index_offset));
        write(_this, sizeof(int64_t), &(column.offset_index_length));
        write(_this, sizeof(int64_t), &(column.column_index_offset));
        write(_this, sizeof(int64_t), &(column.column_index_length));
    }
}

static void writeColumnMetaDatas(MetaDataWriter *_this, unsigned short num_metas, ColumnMetaData *columnMetaDatas) {
    int i;
    for (i = 0; i < num_metas; ++i) {
        ColumnMetaData columnMetaData = columnMetaDatas[i];
        write(_this, sizeof(Type), &(columnMetaData.type));
        write(_this, sizeof(unsigned short), &(columnMetaData.encoding_len));
        write(_this, columnMetaData.encoding_len * sizeof(Encoding), columnMetaData.encodings);
        write(_this, sizeof(unsigned short), &(columnMetaData.path_len));
        writeImmutableStrings(_this, columnMetaData.path_len, columnMetaData.path_in_schema);
        write(_this, sizeof(CompressionCodec), &(columnMetaData.codec));
        write(_this, sizeof(int64_t), &(columnMetaData.num_values));
        write(_this, sizeof(int64_t), &(columnMetaData.total_uncompressed_size));
        write(_this, sizeof(int64_t), &(columnMetaData.total_compressed_size));

        write(_this, sizeof(int32_t), &(columnMetaData.kv_len));
        writeKeyValues(_this, columnMetaData.kv_len, columnMetaData.key_value_metadata);

        write(_this, sizeof(int64_t), &(columnMetaData.data_page_offset));
        write(_this, sizeof(int64_t), &(columnMetaData.index_page_offset));
        write(_this, sizeof(int64_t), &(columnMetaData.dictionary_page_offset));


        writeStatistics(_this, 1, &(columnMetaData.statistics));

        write(_this, sizeof(unsigned short), &(columnMetaData.stat_len));
        writePageEncodingStats(_this, 1, columnMetaData.encoding_stats);
    }
}

static void writeKeyValues(MetaDataWriter *_this, int32_t num_kvs, KeyValue *kvs) {
    int i;
    for (i = 0; i < num_kvs; ++i) {
        KeyValue kv = kvs[i];
        writeImmutableStrings(_this, 1, &(kv.key));
        writeImmutableStrings(_this, 1, &(kv.value));
    }
}

static void writeStatistics(MetaDataWriter *_this, int32_t num_statistics, Statistics *statistics) {
    int i;
    for (i = 0; i < num_statistics; ++i) {
        Statistics s = statistics[i];
        writeImmutableStrings(_this, 1, &(s.max));
        writeImmutableStrings(_this, 1, &(s.min));
        write(_this, sizeof(int64_t), &(s.null_count));
        write(_this, sizeof(int64_t), &(s.distinct_count));
        writeImmutableStrings(_this, 1, &(s.max_value));
        writeImmutableStrings(_this, 1, &(s.min_value));
    }
}

static void
writePageEncodingStats(MetaDataWriter *_this, unsigned short num_stats, PageEncodingStats *encoding_stats) {
    int i;
    for (i = 0; i < num_stats; ++i) {
        PageEncodingStats stats = encoding_stats[i];
        write(_this, sizeof(PageType), &(stats.page_type));
        write(_this, sizeof(Encoding), &(stats.encoding));
        write(_this, sizeof(int32_t), &(stats.count));
    }
}

static void
writeSortingColumns(MetaDataWriter *_this, unsigned short num_sorting_columns, SortingColumn *sortingColumns) {
    int i;
    for (i = 0; i < num_sorting_columns; ++i) {
        SortingColumn sortingColumn = sortingColumns[i];
        write(_this, sizeof(int32_t), &(sortingColumn.column_idx));
        write(_this, sizeof(bool), &(sortingColumn.descending));
        write(_this, sizeof(bool), &(sortingColumn.nulls_first));
    }
}

static void writeImmutableStrings(MetaDataWriter *_this, int32_t num_str, const String *strings) {
    int i;
    for (i = 0; i < num_str; ++i) {
        String str = strings[i];
        write(_this, sizeof(unsigned short), &(str.length));
        write(_this, str.length * sizeof(char), str.str);
    }
}