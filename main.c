//
// Created by wangpengyu6 on 18-5-3.
//

#include <api/reader.h>
#include <api/writer.h>
#include "idx_constants.h"
#include "src/common/idx_types.h"

void testStatistics(void) {
    initConstants();
    immutable_string str = {4, "1234"};

    Statistics *s1 = initStatistics();
    Statistics *s2 = initStatistics_(empty, empty, 0, 0, empty, empty);

    printf("%d\n", s1->equals(s1, s2));

    char str1[95], str2[95];
    s1->toString(s1->This, str1);
    s2->toString(s2->This, str2);
    printf("s1: %s\n", str1);
    printf("s2: %s\n", str2);
    s1->free(s1);
    s2->free(s2);
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