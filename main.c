//
// Created by wangpengyu6 on 18-5-3.
//

#include "idx_constants.h"
#include "src/common/idx_types.h"

int main(void) {

    initConstants();
    immutable_string str = {4, "1234"};

    Statistics *s1 = initStatistics();
    Statistics *s2 = initStatistics_(empty, empty, 0, 0, empty, empty);

    printf("%d\n", s1->equals(s1, s2));
    s1->free(s1);
    s2->free(s2);
    return 0;
}