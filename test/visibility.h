//
// Created by wangpengyu6 on 18-4-28.
//

#ifndef INDEXED_FILE_VISIBILITY_H
#define INDEXED_FILE_VISIBILITY_H

#ifndef INDEXED_EXPORT
#define INDEXED_EXPORT __attribute__((visibility("default")))
#endif

#ifndef INDEXED_NO_EXPORT
#define INDEXED_NO_EXPORT __attribute__((visibility("hidden")))
#endif

#if defined(__clang__)
#define INDEXED_EXTERN_TEMPLATE extern template class INDEXED_EXPORT
#else
#define INDEXED_EXTERN_TEMPLATE extern template class
#endif

#ifdef _MSC_VER
#define INDEXED_TEMPLATE_EXPORT INDEXED_EXPORT
#else
#define INDEXED_TEMPLATE_EXPORT
#endif

#endif //INDEXED_FILE_VISIBILITY_H
