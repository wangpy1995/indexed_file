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

void testMetaDataWriter(void) {
    FILE *fp = fopen("../resources/test_idx_file", "rb+");
    MetaDataWriter *idxWriter = createMetaDataWriter(fp);
    size_t startPos = idxWriter->pos;
    ColumnOrder order = {};
    FileMetaData meta = {
            .version=001,
            .num_schemas=1,
            .num_rows=0,
            .num_groups=0,
            .num_kvs=2,
            .created_by={.str="wpy", .length=3},
            .num_column_orders=1,
            .column_orders=&order
    };

    StringType strType = {};
    LogicalType logic = {
            .STRING=strType
    };

    //schema
    meta.schema = malloc(meta.num_schemas * sizeof(SchemaElement));
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


    meta.key_value_metadata = malloc(meta.num_kvs * sizeof(KeyValue));

    meta.key_value_metadata[0].key.length = 2;
    meta.key_value_metadata[0].key.str = "k1";
    meta.key_value_metadata[0].value.length = 2;
    meta.key_value_metadata[0].value.str = "v1";

    meta.key_value_metadata[1].key.length = 2;
    meta.key_value_metadata[1].key.str = "t2";
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
    MetaDataReader *idxReader = createMetaDataReader(fp);

    FileMetaData *meta = malloc(sizeof(FileMetaData));
    idxReader->readFileMeta(idxReader, meta);
    printf("meta: %p\n", meta);
    freeFileMetaData(&meta);
    idxReader->close(idxReader);
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
            fclose(fp);
            int fd = open("../resources/test_idx_file", O_RDONLY);
            const char *buffer = mmap(NULL, footIndexLength, PROT_READ, MAP_SHARED, fd,
                                      fileLength - MAGIC.length - sizeof(int) - footIndexLength);
            close(fd);

            FileMetaBuffer *fbuffer = createFileMetaBuffer(buffer);

//            FileMetaData metaData = {
//                    .version=*((int *) buffer),
//                    .num_schemas =*((unsigned short *) (buffer + 2))
//            };
            printf("meta: %p\n", fbuffer->fileMeta);
            fbuffer->freeFileMeta(fbuffer);

            printf("%s\n",buffer);
        } else {
            printf("expected MAGIC string: IDX1, actual %s.\n", magic);
        }
    }
}

int main(void) {
//    testMetaDataWriter();
//    testIO();
//    testMetaDataReader();
    testReadAll();
//    printf("%ld\n", sizeof(struct Test));
    return 0;
}