//
// Created by wangpengyu6 on 18-5-25.
//

#include "page_header_writer.h"
#include <stdlib.h>
#include <common/idx_types.h>
#include <common/types/page_header.h>

static PageHeaderWriter *createPageHeaderWriter(FILE *fp, size_t pos) {
    if (fp) {
        PageHeaderWriter *pageHeaderWriter = malloc(sizeof(PageHeaderWriter));
        pageHeaderWriter->fp = fp;
        pageHeaderWriter->pos = pos;

        return pageHeaderWriter;
    } else {
        return NULL;
    }
}

static void write(PageHeaderWriter *_this, size_t size, void *data) {
    if (_this) {
        fseek(_this->fp, _this->pos, SEEK_SET);
        fwrite(data, size, 1, _this->fp);
        _this->pos += size;
    }
}

static void writePageHeader(PageHeaderWriter *_this, PageHeader *pageHeader) {

}

static void writeDataPageHeader()

        static void writeDataPageHeader(PageHeaderWriter *_this) {
    if (_this) {

    }
}

static void writeIndexPageHeader(PageHeaderWriter *_this) {
    if (_this) {

    }
}

static void writeDictionaryPageHeader(PageHeaderWriter *_this) {
    if (_this) {

    }
}