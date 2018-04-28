//
// Created by wangpengyu6 on 18-4-28.
//

#ifndef INDEXED_FILE_STATUS_H
#define INDEXED_FILE_STATUS_H

#include <cstring>
#include <iosfwd>
#include <string>

#include <cwchar>
#include "util/visibility.h"

#define INDEXED_PREDICT_FALSE(x) (__builtin_expect(x, 0))

#define INDEXED_RETURN_NOT_OK(s) \
do{             \
::indexed::Status _s = (s);     \
 if (INDEXED_PREDICT_FALSE(!_s.ok())) { \
      return _s;                         \
    }                                    \
  } while (false)

namespace indexed {

    enum StatusCode : char {
        OK = 0,
        OutOfMemory = 1,
        KeyError = 2,
        TypeError = 3,
        Invalid = 4,
        IOError = 5,
        UnknownError = 9,
        NotImplemented = 10,
        SerializationError = 11,
        PythonError = 12,
        PlasmaObjectExists = 20,
        PlasmaObjectNonexistent = 21,
        PlasmaStoreFull = 22
    };

    class INDEXED_EXPORT Status {
    public:
        Status() : state_(NULL) {}

        ~Status() { delete state_; }

        Status(StatusCode code, const std::string &msg);

        Status(const Status &s);

        void operator=(const Status &s);

        static Status OK() {
            return Status();
        }

        static Status OutOfMemory(const std::string &msg) {
            return Status(StatusCode::OutOfMemory, msg);
        }

        static Status KeyError(const std::string &msg) {
            return Status(StatusCode::KeyError, msg);
        }

        static Status TypeError(const std::string &msg) {
            return Status(StatusCode::TypeError, msg);
        }

        static Status Invalid(const std::string &msg) {
            return Status(StatusCode::Invalid, msg);
        }

        static Status UnknownError(const std::string &msg) {
            return Status(StatusCode::UnknownError, msg);
        }

        static Status NotImplemented(const std::string &msg) {
            return Status(StatusCode::NotImplemented, msg);
        }

        static Status SerializationError(const std::string &msg) {
            return Status(StatusCode::SerializationError, msg);
        }

        static Status PythonError(const std::string &msg) {
            return Status(StatusCode::PythonError, msg);
        }

        static Status PlasmaObjectExists(const std::string &msg) {
            return Status(StatusCode::PlasmaObjectExists, msg);
        }

        static Status PlasmaObjectNonexistent(const std::string &msg) {
            return Status(StatusCode::PlasmaObjectNonexistent, msg);
        }

        static Status PlasmaStoreFull(const std::string &msg) {
            return Status(StatusCode::PlasmaStoreFull, msg);
        }

        bool ok() const { return state_ == NULL; }

        bool IsOutOfMemory() const { return code() == StatusCode::OutOfMemory; }

        bool IsKeyError() const { return code() == StatusCode::KeyError; }

        bool IsInvalid() const { return code() == StatusCode::Invalid; }

        bool IsIOError() const { return code() == StatusCode::IOError; }

        bool IsTypeError() const { return code() == StatusCode::TypeError; }

        bool IsUnknownError() const { return code() == StatusCode::UnknownError; }

        bool IsNotImplemented() const { return code() == StatusCode::NotImplemented; }

        // An object could not be serialized or deserialized.
        bool IsSerializationError() const { return code() == StatusCode::SerializationError; }

        // An error is propagated from a nested Python function.
        bool IsPythonError() const { return code() == StatusCode::PythonError; }

        // An object with this object ID already exists in the plasma store.
        bool IsPlasmaObjectExists() const { return code() == StatusCode::PlasmaObjectExists; }

        // An object was requested that doesn't exist in the plasma store.
        bool IsPlasmaObjectNonexistent() const {
            return code() == StatusCode::PlasmaObjectNonexistent;
        }

        // An object is too large to fit into the plasma store.
        bool IsPlasmaStoreFull() const { return code() == StatusCode::PlasmaStoreFull; }

        std::string ToString() const;

        std::string CodeAsString() const;

        StatusCode code() const {
            return ok() ? StatusCode::OK : state_->code;
        }

        std::string message() const {
            return ok() ? "" : state_->msg;
        }

    private:
        struct State {
            StatusCode code;
            std::string msg;
        };

        State *state_;

        void CopyFrom(const State *s);
    };


    static inline std::ostream &operator<<(std::ostream &os, const Status &x) {
        os << x.ToString();
        return os;
    }

    inline Status::Status(const Status &s)
            : state_((s.state_ == NULL) ? NULL : new State(*s.state_)) {}

    inline void Status::operator=(const Status &s) {
        // The following condition catches both aliasing (when this == &s),
        // and the common case where both s and *this are ok.
        if (state_ != s.state_) {
            CopyFrom(s.state_);
        }
    }
}

#endif //INDEXED_FILE_STATUS_H
