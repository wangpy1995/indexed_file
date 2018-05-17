//
// Created by wpy on 18-5-18.
//

#include <common/idx_constants.h>
#include "writer.h"
#include "schema.h"

void writeFooter(FILE *file) {
    if (file) {
        int footerIndexLength = 0;
        fwrite(&footerIndexLength, sizeof(int), 1, file);
        fwrite(MAGIC.str, MAGIC.length, 1, file);
        fflush(file);
    }
}