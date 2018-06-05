//
// Created by wangpengyu6 on 18-5-3.
//

#include <common/io/file_meta_writer.h>
#include <common/io/file_meta_reader.h>
#include <common/types/meta_data.h>
#include <common/types/schema.h>
#include <api/reader.h>
#include <common/types/statistics.h>
#include <idx_constants.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <zconf.h>
#include <common/io/file_meta_buffer.h>

void testIO() {
    FILE *file;
//    FILE *file = fopen("../resources/test.dat", "rb+");
//    writeFooter(file);
//    if (file)fclose(file);
    file = fopen("../resources/test_idx_file", "rb+");
    readFooter(file);
    if (file)fclose(file);
}

void mockColumnChunk(ColumnChunk *column) {

    column->file_path.length = 59;
    column->file_path.str = malloc(column->file_path.length * sizeof(char));
    column->file_path.str = "/home/wangpengyu6/CLionProjects/indexed_file/resources/test_idx_file";
    column->file_offset = 0;

    column->meta_data.type = FIXED_LEN_BYTE_ARRAY;
    column->meta_data.num_values = 100;
    column->meta_data.codec = LZ4;
    column->meta_data.total_uncompressed_size = 100;
    column->meta_data.total_compressed_size = 3;
    column->meta_data.kv_len = 0;
    column->meta_data.data_page_offset = 0;
    column->meta_data.index_page_offset = 0;
    column->meta_data.dictionary_page_offset = 0;
    column->meta_data.statistics.distinct_count = 3;
    column->meta_data.statistics.null_count = 2;

    column->meta_data.path_len = 1;
    column->meta_data.path_in_schema = malloc(column->meta_data.path_len * sizeof(String));
    column->meta_data.path_in_schema->length = 4;
    column->meta_data.path_in_schema->str = malloc(column->meta_data.path_in_schema->length * sizeof(char));
    column->meta_data.path_in_schema->str = "test";

    column->meta_data.stat_len = 2;
    column->meta_data.encoding_stats = malloc(column->meta_data.stat_len * sizeof(PageEncodingStats));
    column->meta_data.encoding_stats[0].page_type = DATA_PAGE_V2;
    column->meta_data.encoding_stats[0].encoding = RLE;
    column->meta_data.encoding_stats[0].count = 10;
    column->meta_data.encoding_stats[1].page_type = DATA_PAGE_V2;
    column->meta_data.encoding_stats[1].encoding = RLE;
    column->meta_data.encoding_stats[1].count = 50;

    column->offset_index_offset = 0;
    column->offset_index_length = 0;
    column->column_index_offset = 0;
    column->column_index_length = 0;
}

void testMetaDataWriter(void) {
    FILE *fp = fopen("../resources/test_idx_file", "rb+");
    TypeDefinedOrder o = {};
    MetaDataWriter *idxWriter = createMetaDataWriter(fp);
    size_t startPos = idxWriter->pos;
    ColumnOrder order = {.TYPE_ORDER=o};
    FileMetaData meta = {
            .version=001,
            .schema_len=1,
            .num_rows=150,
            .group_len=1,
            .kv_len=2,
            .created_by={.str="wpy", .length=3},
            .order_len=1,
            .column_orders=&order
    };

    StringType strType = {};
    LogicalType logic = {
            .STRING=strType
    };

    //schema
    meta.schema = malloc(meta.schema_len * sizeof(SchemaElement));
    meta.schema->type = BYTE_ARRAY;
    meta.schema->type_length = 4;
    meta.schema->repetition_type = REPEATED;

    meta.schema->name.length = 4;
    meta.schema->name.str = "test";

    meta.schema->num_children = 0;
    meta.schema->converted_type = JSON;
    meta.schema->scale = 0;
    meta.schema->precision = 0;
    meta.schema->field_id = 0;
    meta.schema->logicalType = logic;

    //row_group
    meta.row_groups = malloc(meta.group_len * sizeof(RowGroup));
    meta.row_groups->num_rows = 100;
    meta.row_groups->sorting_columns_len = 2;
    meta.row_groups->sorting_columns = malloc(meta.row_groups->sorting_columns_len * sizeof(SortingColumn));
    meta.row_groups->sorting_columns[0].column_idx = 0;
    meta.row_groups->sorting_columns[0].descending = true;
    meta.row_groups->sorting_columns[0].nulls_first = false;
    meta.row_groups->sorting_columns[1].column_idx = 1;
    meta.row_groups->sorting_columns[1].descending = false;
    meta.row_groups->sorting_columns[1].nulls_first = true;

    meta.row_groups->chunk_len = 2;
    meta.row_groups->columns = malloc(meta.row_groups->chunk_len * sizeof(ColumnChunk));
    mockColumnChunk(meta.row_groups->columns);
    mockColumnChunk(meta.row_groups->columns + 1);


    //key_value
    meta.key_value_metadata = malloc(meta.kv_len * sizeof(KeyValue));

    meta.key_value_metadata[0].key.length = 2;
    meta.key_value_metadata[0].key.str = "k1";
    meta.key_value_metadata[0].value.length = 2;
    meta.key_value_metadata[0].value.str = "v1";

    meta.key_value_metadata[1].key.length = 2;
    meta.key_value_metadata[1].key.str = "k2";
    meta.key_value_metadata[1].value.length = 2;
    meta.key_value_metadata[1].value.str = "v2";

    idxWriter->writeFileMeta(idxWriter, meta);
    idxWriter->flush(idxWriter);

    size_t metaLength = idxWriter->pos - startPos;
    //fileMetaLength
    idxWriter->write(idxWriter, sizeof(int32_t), &metaLength);
    idxWriter->write(idxWriter, sizeof(int32_t), "IDX1");

    idxWriter->close(idxWriter);
    free(meta.schema);
}

void testMetaDataReader(void) {
    FILE *fp = fopen("../resources/test_idx_file", "rb+");
    fseek(fp, 0, SEEK_END);
    size_t fileLength = ftell(fp);
    size_t start_pos = fileLength - MAGIC.length;
    fseek(fp, start_pos, SEEK_SET);
    char magic[MAGIC.length];
    fread(magic, MAGIC.length, 1, fp);
    if (*((int *) MAGIC.str) == (*((int *) magic))) {
        int footIndexLength;
        start_pos -= sizeof(int);
        fseek(fp, start_pos, SEEK_SET);
        fread(&footIndexLength, sizeof(int), 1, fp);
        start_pos -= footIndexLength;
        MetaDataReader *idxReader = createMetaDataReader(fp, start_pos, footIndexLength);

        FileMetaData *meta = malloc(sizeof(FileMetaData));
        idxReader->readFileMeta(idxReader, meta, SKIP_SCHEMAS);
        printf("meta: %p\n", meta);
        freeFileMetaData(&meta);
        idxReader->close(idxReader);
    }
}

void testReadAll(void) {
    FILE *fp;
    fp = fopen("../resources/test_idx_file", "rb+");
    if (fp) {
        fseek(fp, 0, SEEK_END);
        long int fileLength = ftell(fp);
        fseek(fp, fileLength - (MAGIC.length), SEEK_SET);
        char magic[MAGIC.length];
        fread(magic, MAGIC.length, 1, fp);
        if (*((int *) MAGIC.str) == (*((int *) magic))) {
            int footIndexLength;
            fseek(fp, fileLength - MAGIC.length - sizeof(int), SEEK_SET);
            fread(&footIndexLength, sizeof(int), 1, fp);
            fseek(fp, fileLength - MAGIC.length - sizeof(int) - 3 * sizeof(int), SEEK_SET);
            fclose(fp);
            int fd = open("../resources/test_idx_file", O_RDONLY);
            const char *buffer = mmap(NULL, footIndexLength, PROT_READ, MAP_SHARED, fd,
                                      fileLength - MAGIC.length - sizeof(int) - footIndexLength);
            close(fd);

            FileMetaBuffer *fbuffer = createFileMetaBuffer(buffer, footIndexLength, SKIP_SCHEMAS);

//            FileMetaData metaData = {
//                    .version=*((int *) buffer),
//                    .schema_len =*((unsigned short *) (buffer + 2))
//            };
            printf("meta: %p\n", fbuffer->fileMeta);
            fbuffer->freeFileMeta(fbuffer);

            printf("%s\n", buffer);
        } else {
            printf("expected MAGIC string: IDX1, actual %s.\n", magic);
        }
    }
}

int main(void) {
    testMetaDataWriter();
//    testIO();
    testMetaDataReader();
//    testReadAll();
    printf("%ld\n", sizeof(String));
    return 0;
}