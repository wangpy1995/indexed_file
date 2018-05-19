//
// Created by wangpengyu6 on 18-5-19.
//

#include <stdlib.h>
#include <common/types/meta_data.h>
#include <common/types/schema.h>
#include "idx_file_writer.h"
#include <stdbool.h>

static void write(IndexedFileWriter *_this, size_t size, void *data);

static void seekTo(IndexedFileWriter *_this, size_t pos);

static void flush(IndexedFileWriter *_this);

static void close(IndexedFileWriter *_this);

static void writeFileMeta(IndexedFileWriter *_this, FileMetaData metaData);

static void writeSchemas(IndexedFileWriter *_this, unsigned short num_schemas, SchemaElement *schemas) ;

static void writeRowGroups(IndexedFileWriter *_this, unsigned short num_groups, RowGroup *rowGroups) ;

static void writeKeyValues(IndexedFileWriter *_this, int32_t num_kvs, KeyValue *kvs) ;

static void writeImmutableStrings(IndexedFileWriter *_this, int32_t num_str, immutable_string *strings) ;

static void writeColumnMetaDatas(IndexedFileWriter *_this, unsigned short num_metas, ColumnMetaData *columnMetaDatas) ;

static void writeColumnChunks(IndexedFileWriter *_this, unsigned short num_columns, ColumnChunk *columns) ;

static void
writeSortingColumns(IndexedFileWriter *_this, unsigned short num_sorting_columns, SortingColumn *sortingColumns) ;

static void writeStatistics(IndexedFileWriter *_this, int32_t num_statistics, Statistics *statistics) ;

static void
writePageEncodingStats(IndexedFileWriter *_this, unsigned short num_stats, PageEncodingStats *encoding_stats) ;

IndexedFileWriter *createIndexedFileWriter(FILE *fp) {
    if (fp) {
        IndexedFileWriter *writer = malloc(sizeof(IndexedFileWriter));
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

static void write(IndexedFileWriter *_this, size_t size, void *data) {
    if (_this && size > 0 && data) {
        fseek((FILE *) _this->fp, _this->pos, SEEK_SET);
        fwrite(data, size, 1, (FILE *) _this->fp);
        _this->pos += size;
    }
}

static void seekTo(IndexedFileWriter *_this, size_t pos) {
    if (_this) {
        _this->pos = pos;
    }
}

static void flush(IndexedFileWriter *_this) {
    if (_this) {
        fflush(_this->fp);
    }
}

static void close(IndexedFileWriter *_this) {
    if (_this) {
        fclose(_this->fp);
        free(_this);
        _this->fp = NULL;
        _this = NULL;
    }
}

static void writeFileMeta(IndexedFileWriter *_this, const FileMetaData metaData) {
    if (_this) {
        //version  int32
        write(_this, sizeof(int32_t), &(metaData.version));
        //schema_len  int16  节省空间  需要单属性写入
        write(_this, sizeof(unsigned short), &(metaData.num_schemas));
        writeSchemas(_this, metaData.num_schemas, metaData.schema);
        //numRows int64
        write(_this, sizeof(int64_t), &(metaData.num_rows));
        //rowGroups
        write(_this, sizeof(unsigned short), &(metaData.num_groups));
        writeRowGroups(_this, metaData.num_groups, metaData.row_groups);
        //kv
        write(_this, sizeof(int32_t), &(metaData.num_kvs));
        writeKeyValues(_this, metaData.num_kvs, metaData.key_value_metadata);
        //createBy
        writeImmutableStrings(_this, 1, &(metaData.created_by));
        //columnOrders
        write(_this, sizeof(unsigned char), &(metaData.num_column_orders));
        write(_this, metaData.num_column_orders* sizeof(ColumnOrder), metaData.column_orders);
    }
}

static void writeSchemas(IndexedFileWriter *_this, unsigned short num_schemas, SchemaElement *schemas) {
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

static void writeRowGroups(IndexedFileWriter *_this, unsigned short num_groups, RowGroup *rowGroups) {
    int i;
    for (i = 0; i < num_groups; ++i) {
        RowGroup rowGroup = rowGroups[i];

        write(_this, sizeof(unsigned short), &(rowGroup.num_columns));
        writeColumnChunks(_this, rowGroup.num_columns, rowGroup.columns);
        write(_this, sizeof(int64_t), &(rowGroup.total_byte_size));
        write(_this, sizeof(int64_t), &(rowGroup.num_rows));
        write(_this, sizeof(int64_t), &(rowGroup.num_sorting_columns));
        write(_this, sizeof(int64_t), &(rowGroup.sorting_columns));
        writeSortingColumns(_this, rowGroup.num_sorting_columns, rowGroup.sorting_columns);
    }
}

static void writeColumnChunks(IndexedFileWriter *_this, unsigned short num_columns, ColumnChunk *columns) {
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

static void writeColumnMetaDatas(IndexedFileWriter *_this, unsigned short num_metas, ColumnMetaData *columnMetaDatas) {
    int i;
    for (i = 0; i < num_metas; ++i) {
        ColumnMetaData columnMetaData = columnMetaDatas[i];
        write(_this, sizeof(Type), &(columnMetaData.type));
        write(_this, sizeof(unsigned short), &(columnMetaData.num_encodings));
        write(_this, columnMetaData.num_encodings * sizeof(Encoding), columnMetaData.encodings);
        write(_this, sizeof(unsigned short), &(columnMetaData.num_paths));
        writeImmutableStrings(_this, columnMetaData.num_paths, columnMetaData.path_in_schema);
        write(_this, sizeof(CompressionCodec), &(columnMetaData.codec));
        write(_this, sizeof(int64_t), &(columnMetaData.num_values));
        write(_this, sizeof(int64_t), &(columnMetaData.total_uncompressed_size));
        write(_this, sizeof(int64_t), &(columnMetaData.total_compressed_size));

        write(_this, sizeof(int32_t), &(columnMetaData.num_kvs));
        writeKeyValues(_this, columnMetaData.num_kvs, columnMetaData.key_value_metadata);

        write(_this, sizeof(int64_t), &(columnMetaData.data_page_offset));
        write(_this, sizeof(int64_t), &(columnMetaData.index_page_offset));
        write(_this, sizeof(int64_t), &(columnMetaData.dictionary_page_offset));


        writeStatistics(_this, 1, &(columnMetaData.statistics));

        write(_this, sizeof(unsigned short), &(columnMetaData.num_stats));
        writePageEncodingStats(_this, 1, columnMetaData.encoding_stats);
    }
}

static void writeKeyValues(IndexedFileWriter *_this, int32_t num_kvs, KeyValue *kvs) {
    int i;
    for (i = 0; i < num_kvs; ++i) {
        KeyValue kv = kvs[i];
        writeImmutableStrings(_this, 1, &(kv.key));
        writeImmutableStrings(_this, 1, &(kv.value));
    }
}

static void writeStatistics(IndexedFileWriter *_this, int32_t num_statistics, Statistics *statistics) {
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
writePageEncodingStats(IndexedFileWriter *_this, unsigned short num_stats, PageEncodingStats *encoding_stats) {
    int i;
    for (i = 0; i < num_stats; ++i) {
        PageEncodingStats stats = encoding_stats[i];
        write(_this, sizeof(PageType), &(stats.page_type));
        write(_this, sizeof(Encoding), &(stats.encoding));
        write(_this, sizeof(int32_t), &(stats.count));
    }
}

static void
writeSortingColumns(IndexedFileWriter *_this, unsigned short num_sorting_columns, SortingColumn *sortingColumns) {
    int i;
    for (i = 0; i < num_sorting_columns; ++i) {
        SortingColumn sortingColumn = sortingColumns[i];
        write(_this, sizeof(int32_t), &(sortingColumn.column_idx));
        write(_this, sizeof(bool), &(sortingColumn.descending));
        write(_this, sizeof(bool), &(sortingColumn.nulls_first));
    }
}

static void writeImmutableStrings(IndexedFileWriter *_this, int32_t num_str, immutable_string *strings) {
    int i;
    for (i = 0; i < num_str; ++i) {
        immutable_string str = strings[i];
        write(_this, sizeof(unsigned short), &(str.length));
        write(_this, str.length * sizeof(char), (void *) str.str);
    }
}