//
// Created by wangpengyu6 on 18-4-28.
//

/**
 * 文件结构
 * File:
 *  --------------
 *  | DataPage |
 *  -------------
 *  |  Footer   |
 *  -------------
 *
 *  DataPage:
 *  -----------------
 *  | RowGroup0 |
 *  ----------------
 *  | RowGroup1 |
 *  ----------------
 *  |       ......      |
 *  ----------------
 *
 *  Footer:
 *  ---------------------
 *  | RowGroupInfo0 |
 *  ---------------------
 *  | RowGroupInfo1 |
 *  ---------------------
 *  |           ......         |
 *  ---------------------
 *  |       Schema      |
 *  ---------------------
 *  |    FootLength   | (4 byte)
 *  ---------------------
 *  |       MAGIC      | (4 byte)
 *  ---------------------
 *
 *  RowGroupInfo:
 *  ---------------------
 *  | ColumnChunk |
 *  ---------------------
 *  | RowCount |
 *  ---------------------
 *  | NullCount |
 *  ---------------------
 *  | DistinctCount |
 *  ---------------------
 */

#ifndef INDEXED_FILE_READER_H
#define INDEXED_FILE_READER_H

#include <stdio.h>

void readFooter(FILE *file);


#endif //INDEXED_FILE_READER_H
