//
// Created by wangpengyu6 on 18-5-3.
//

#include "api/reader.h"
#include "common/types/statistics.h"
#include "idx_constants.h"
#include "src/common/idx_types.h"

void testStatistics(void) {
    initConstants();
    immutable_string str = {4, "1234"};

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

int main(void) {
    testIO();
    return 0;
}