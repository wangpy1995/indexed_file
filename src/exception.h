//
// Created by wangpengyu6 on 18-4-28.
//

#include <exception>
#include <string>
#include "common/status.h"

#ifndef INDEXED_FILE_EXCEPTION_H
#define INDEXED_FILE_EXCEPTION_H

//TODO
#define INDEXED_CATCH_NOT_OK(s)                    \
  try {                                            \
    (s);                                           \
  } catch (const ::indexed::IndexedException& e) { \
    return ::arrow::Status::IOError(e.what());     \
  }

#define INDEXED_IGNORE_NOT_OK(s) \
  do {                           \
    ::arrow::Status _s = (s);    \
    ARROW_UNUSED(_s);            \
  } while (0)

#define INDEXED_THROW_NOT_OK(s)                     \
  do {                                              \
    ::arrow::Status _s = (s);                       \
    if (!_s.ok()) {                                 \
      std::stringstream ss;                         \
      ss << "Arrow error: " << _s.ToString();       \
      ::indexed::IndexedException::Throw(ss.str()); \
    }                                               \
  } while (0)

namespace indexed {
    class IndexedException : public std::exception {
    public:
        static void EofException(const std::string &msg = "");

        static void NYI(const std::string &msg);

        static void Throw(const std::string &msg);

        explicit IndexedException(const char *msg);

        explicit IndexedException(const std::string &msg);

        explicit IndexedException(const std::string &msg, exception &e);

        virtual ~IndexedException() throw();

        virtual const char* what() const throw();

    private:
        std::string msg_;
    };
}

#endif //INDEXED_FILE_EXCEPTION_H
