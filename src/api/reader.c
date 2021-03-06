//
// Created by wpy on 18-5-18.
//

#include <stdio.h>
#include <string.h>
#include "reader.h"
#include "schema.h"

void readFooter(FILE *file) {
    if (file) {
        fseek(file, 0, SEEK_END);
        long int fileLength = ftell(file);
        fseek(file, fileLength - (MAGIC.length), SEEK_SET);
        char magic[MAGIC.length];
        fread(magic, MAGIC.length, 1, file);
        if (*((int *) MAGIC.str) == (*((int *) magic))) {
            int footIndexLength;
            fseek(file,fileLength - MAGIC.length - sizeof(int),SEEK_SET);
            fread(&footIndexLength, sizeof(int), 1, file);
            printf("foot index length: %d\n", footIndexLength);
        } else {
            printf("expected MAGIC string: IDX1, actual %s.\n", magic);
        }
    }
};