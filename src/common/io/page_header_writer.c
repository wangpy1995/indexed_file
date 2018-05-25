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
    if (_this) {
        write(_this, sizeof(PageType), pageHeader);
        write(_this, sizeof(int32_t), &(pageHeader->uncompressed_page_size));
        write(_this, sizeof(int32_t), &(pageHeader->compressed_page_size));
        write(_this, sizeof(int32_t), &(pageHeader->crc));

        // write DataPageHeader
        if(pageHeader->type==DATA_PAGE) {
            write(_this, sizeof(int32_t), &(pageHeader->data_page_header.num_values));
            write(_this, sizeof(Encoding),&(pageHeader->data_page_header.encoding));
            write(_this, sizeof(Encoding),&(pageHeader->data_page_header.definition_level_encoding));
            write(_this, sizeof(Encoding),&(pageHeader->data_page_header.repetition_level_encoding));
            writeStatistics()
        }

    }
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