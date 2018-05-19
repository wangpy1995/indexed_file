//
// Created by wangpengyu6 on 18-5-3.
//

#include <common/io/idx_file_reader.h>
#include <common/io/idx_file_writer.h>
#include <common/types/meta_data.h>
#include <common/types/schema.h>
#include "api/reader.h"
#include "common/types/statistics.h"
#include "idx_constants.h"
#include "src/common/idx_types.h"

void testStatistics(void) {
    initConstants();
    immutable_string str = {.str= "1234", .length=4};

    Statistics s1 = STATISTICS;
    Statistics *s2 = initStatistics(EMPTY_STRING, str, -1, 0, EMPTY_STRING, EMPTY_STRING);
    printf("%d\n", s1.equals(&s1, s2));

    char str1[95], str2[95];
    if (&s1) {
        s1.mkString(&s1, str1);
        printf("s1: %s\n", str1);
        s1.free(&s1);
    }
    if (s2) {
        s2->mkString(s2, str2);
        printf("s2: %s\n", str2);
        s2 = s2->free(s2);
    }
}

void testIO() {
    FILE *file;
//    FILE *file = fopen("../resources/test.dat", "rb+");
//    writeFooter(file);
//    if (file)fclose(file);
    file = fopen("../resources/test.dat", "rb+");
    readFooter(file);
    if (file)fclose(file);
}

void testIndexedFileWriter(void) {
    FILE *fp = fopen("../resources/test_idx_file", "rb+");
    IndexedFileWriter *idxWriter = createIndexedFileWriter(fp);
    FileMetaData meta = {
            .version=001,
            .schema_length=sizeof(SchemaElement),
            .num_rows=0,
            .group_length=0,
            .kv_length=2 * sizeof(KeyValue),
            .created_by={.str="wpy", .length=3},
            .column_order_length=0
    };

    StringType strType = {};
    LogicalType logic = {
            .STRING=strType
    };

    //schema
    meta.schema = malloc(meta.schema_length);
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


    meta.key_value_metadata = malloc((size_t) meta.kv_length);

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
    idxWriter->close(idxWriter);

    free(meta.schema);
    meta.schema = NULL;
}

void testIndexedFileReader(void) {
    FILE *fp = fopen("../resources/test_idx_file", "rb+");
    IndexedFileReader *idxReader = createIndexedFileReader(fp);
    const FileMetaData *meta = idxReader->readFileMeta(idxReader);
    printf("meta: %p\n", meta);
    if(meta->schema){
        free(meta->schema);
    }
    if(meta->row_groups){
        free(meta->row_groups);
    }
    if(meta->column_orders){
        free(meta->column_orders);
    }
    if(meta->key_value_metadata){
        free(meta->key_value_metadata);
    }
    meta=NULL;
    idxReader->close(idxReader);
}

int main(void) {
//    testIO();
//    testIndexedFileWriter();
    testIndexedFileReader();
//    printf("%ld\n", sizeof(struct Test));
    return 0;
}