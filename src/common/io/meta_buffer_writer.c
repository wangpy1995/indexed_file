//
// Created by wangpengyu6 on 18-5-25.
//


////////////////////////////////////////////////
//      write buffer接口
////////////////////////////////////////////////

#include <string.h>

//赋值时直接拷贝内存
static void write(FileMetaBuffer *_this, size_t size, void *data) {
    char *buff = (char *) (_this->buff + _this->pos);
    memcpy(buff, data, size);
    _this->pos += size;
}