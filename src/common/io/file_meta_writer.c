//
// Created by wangpengyu6 on 18-5-19.
//

#include <stdlib.h>
#include <common/types/meta_data.h>
#include <common/types/schema.h>
#include "file_meta_writer.h"
#include <stdbool.h>
#include <common/types/page_header.h>

/**
 *
 * @param _this
 * @param size  需要大于0
 * @param data   不可为null
 */
//内部使用  不再对外开放
static inline void __attribute__((nonnull(1, 3))) write(MetaDataWriter *_this, size_t size, const void *data) {
    fseek(_this->fp, _this->pos, SEEK_SET);
    fwrite(data, size, 1, _this->fp);
    _this->pos += size;
}

//内部使用  不再对外开放
static inline void __attribute__((nonnull(1))) seekTo(MetaDataWriter *_this, size_t pos) {
    _this->pos = pos;
}

//内部使用  不再对外开放
static inline void __attribute__((nonnull(1))) flush(MetaDataWriter *_this) {
    fflush(_this->fp);
}

//内部使用  不再对外开放
static inline void __attribute__((nonnull(1))) close(MetaDataWriter *_this) {
    fclose(_this->fp);
    free(_this);
    _this->fp = NULL;
}


//write
static void writeImmutableStrings(MetaDataWriter *_this, int32_t num_str, const String *strings) {
    int i;
    for (i = 0; i < num_str; ++i) {
        const String *str = strings + i;
        write(_this, sizeof(unsigned short), &(str->length));
        write(_this, str->length * sizeof(char), str->str);
    }
}

static void
writeSortingColumns(MetaDataWriter *_this, unsigned short num_sorting_columns, SortingColumn *sortingColumns) {
    int i;
    for (i = 0; i < num_sorting_columns; ++i) {
        SortingColumn *sortingColumn = sortingColumns + i;
        write(_this, sizeof(int32_t), &(sortingColumn->column_idx));
        write(_this, sizeof(bool), &(sortingColumn->descending));
        write(_this, sizeof(bool), &(sortingColumn->nulls_first));
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

static void writeStatistics(MetaDataWriter *_this, int32_t num_statistics, const Statistics *statistics) {
    int i;
    for (i = 0; i < num_statistics; ++i) {
        const Statistics *s = statistics + i;
        writeImmutableStrings(_this, 1, &(s->max));
        writeImmutableStrings(_this, 1, &(s->min));
        write(_this, sizeof(int64_t), &(s->null_count));
        write(_this, sizeof(int64_t), &(s->distinct_count));
        writeImmutableStrings(_this, 1, &(s->max_value));
        writeImmutableStrings(_this, 1, &(s->min_value));
    }
}

static void writeKeyValues(MetaDataWriter *_this, int32_t num_kvs, KeyValue *kvs) {
    int i;
    for (i = 0; i < num_kvs; ++i) {
        KeyValue *kv = kvs + i;
        writeImmutableStrings(_this, 1, &(kv->key));
        writeImmutableStrings(_this, 1, &(kv->value));
    }
}

static void writeColumnMetaDatas(MetaDataWriter *_this, unsigned short num_metas, ColumnMetaData *columnMetaDatas) {
    int i;
    for (i = 0; i < num_metas; ++i) {
        ColumnMetaData *columnMetaData = columnMetaDatas + i;
        write(_this, sizeof(Type), &(columnMetaData->type));
        write(_this, sizeof(unsigned short), &(columnMetaData->encoding_len));
        write(_this, columnMetaData->encoding_len * sizeof(Encoding), columnMetaData->encodings);
        write(_this, sizeof(unsigned short), &(columnMetaData->path_len));
        writeImmutableStrings(_this, columnMetaData->path_len, columnMetaData->path_in_schema);
        write(_this, sizeof(CompressionCodec), &(columnMetaData->codec));
        write(_this, sizeof(int64_t), &(columnMetaData->num_values));
        write(_this, sizeof(int64_t), &(columnMetaData->total_uncompressed_size));
        write(_this, sizeof(int64_t), &(columnMetaData->total_compressed_size));

        write(_this, sizeof(int32_t), &(columnMetaData->kv_len));
        writeKeyValues(_this, columnMetaData->kv_len, columnMetaData->key_value_metadata);

        write(_this, sizeof(int64_t), &(columnMetaData->data_page_offset));
        write(_this, sizeof(int64_t), &(columnMetaData->index_page_offset));
        write(_this, sizeof(int64_t), &(columnMetaData->dictionary_page_offset));


        writeStatistics(_this, 1, &(columnMetaData->statistics));

        write(_this, sizeof(unsigned short), &(columnMetaData->stat_len));
        writePageEncodingStats(_this, columnMetaData->stat_len, columnMetaData->encoding_stats);
    }
}

static void writeColumnChunks(MetaDataWriter *_this, unsigned short num_columns, ColumnChunk *columns) {
    int i;
    for (i = 0; i < num_columns; ++i) {
        ColumnChunk *column = columns + i;
        writeImmutableStrings(_this, 1, &(column->file_path));
        write(_this, sizeof(int64_t), &(column->file_offset));
        writeColumnMetaDatas(_this, 1, &(column->meta_data));
        write(_this, sizeof(int64_t), &(column->offset_index_offset));
        write(_this, sizeof(int64_t), &(column->offset_index_length));
        write(_this, sizeof(int64_t), &(column->column_index_offset));
        write(_this, sizeof(int64_t), &(column->column_index_length));
    }
}

static void writeRowGroups(MetaDataWriter *_this, unsigned short num_groups, RowGroup *rowGroups) {
    int i;
    for (i = 0; i < num_groups; ++i) {
        RowGroup *rowGroup = rowGroups + i;

        write(_this, sizeof(unsigned short), &(rowGroup->chunk_len));
        writeColumnChunks(_this, rowGroup->chunk_len, rowGroup->columns);
        write(_this, sizeof(int64_t), &(rowGroup->total_byte_size));
        write(_this, sizeof(int64_t), &(rowGroup->num_rows));
        write(_this, sizeof(int64_t), &(rowGroup->sorting_columns_len));
        writeSortingColumns(_this, rowGroup->sorting_columns_len, rowGroup->sorting_columns);
    }
}

static void writeSchemas(MetaDataWriter *_this, unsigned short num_schemas, SchemaElement *schemas) {
    int i;
    for (i = 0; i < num_schemas; ++i) {
        SchemaElement *schema = schemas + i;
        //name
        write(_this, sizeof(unsigned short), &(schema->name.length));
        write(_this, (schemas[i]).name.length * sizeof(char), (void *) (schema->name.str));

        //type
        write(_this, sizeof(Type), &(schema->type));

        //type_length
        write(_this, sizeof(int32_t), &(schema->type_length));

        //repetition_type
        write(_this, sizeof(FieldRepetitionType), &(schema->repetition_type));

        //num_children
        write(_this, sizeof(int32_t), &(schema->num_children));

        //convertedType
        write(_this, sizeof(ConvertedType), &(schema->converted_type));

        //scale
        write(_this, sizeof(int32_t), &(schema->scale));

        //precision
        write(_this, sizeof(int32_t), &(schema->precision));

        //field_id
        write(_this, sizeof(int32_t), &(schema->field_id));

        //logicalType
        write(_this, sizeof(LogicalType), &(schema->logicalType));
    }
}

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
        write(_this, metaData.order_len* sizeof(ColumnOrder), metaData.column_orders);

        //add schema_size | group_size | kv_size  to tail
        write(_this, sizeof(int32_t) * 3, len);
    }
}

static void writeDataPage(MetaDataWriter *_this, const DataPageHeader *header) {
    write(_this, sizeof(int32_t), &(header->num_values));
    write(_this, sizeof(Encoding), &(header->encoding));
    write(_this, sizeof(Encoding), &(header->definition_level_encoding));
    write(_this, sizeof(Encoding), &(header->repetition_level_encoding));
    writeStatistics(_this, 1, &(header->statistics));
}

static void writeDataPageV2(MetaDataWriter *_this, const DataPageHeaderV2 *header) {
    write(_this, sizeof(int32_t), &(header->num_values));
    write(_this, sizeof(int32_t), &(header->num_nulls));
    write(_this, sizeof(int32_t), &(header->num_rows));
    write(_this, sizeof(Encoding), &(header->encoding));
    write(_this, sizeof(int32_t), &(header->definition_levels_byte_length));
    write(_this, sizeof(int32_t), &(header->repetition_levels_byte_length));
    write(_this, sizeof(bool), &(header->is_compressed));
    writeStatistics(_this, 1, &(header->statistics));
}

static void writeIndexPage(MetaDataWriter *_this, const IndexPageHeader *pageHeader) {
}

static void writeDictionaryPage(MetaDataWriter *_this, const DictionaryPageHeader *header) {
    write(_this, sizeof(int32_t), &(header->num_values));
    write(_this, sizeof(Encoding), &(header->encoding));
    write(_this, sizeof(bool), &(header->is_sorted));
}

static void writePageHeader(MetaDataWriter *_this, const PageHeader *pageHeader, int64_t offset) {
    if (_this) {
        size_t cur = _this->pos;
        //移动至offset处
        _this->pos = offset;
        write(_this, sizeof(PageType), &(pageHeader->type));
        write(_this, sizeof(int32_t), &(pageHeader->uncompressed_page_size));
        write(_this, sizeof(int32_t), &(pageHeader->compressed_page_size));
        write(_this, sizeof(int32_t), &(pageHeader->crc));

        //page  顺序写入
        switch (pageHeader->type) {
            case DATA_PAGE:
                writeDataPage(_this, &(pageHeader->data_page_header));
                break;
            case DATA_PAGE_V2:
                writeDataPageV2(_this, &(pageHeader->data_page_header_v2));
                break;
            case INDEX_PAGE:
                writeIndexPage(_this, &(pageHeader->index_page_header));
                break;
            case DICTIONARY_PAGE:
                writeDictionaryPage(_this, &(pageHeader->dictionary_page_header));
                break;
            default:
                break;
        }
        _this->pos = cur;
    }
}

MetaDataWriter *createMetaDataWriter(FILE *fp) {
    if (fp) {
        MetaDataWriter *writer = malloc(sizeof(MetaDataWriter));
        writer->fp = fp;
        writer->pos = 0;
        writer->writeFileMeta = writeFileMeta;
        writer->writePageHeader = writePageHeader;
        writer->write = write;
        writer->seekTo = seekTo;
        writer->flush = flush;
        writer->close = close;
        return writer;
    }
    return NULL;
}