//
// Created by wangpengyu6 on 18-4-28.
//

#include <cassert>
#include "status.h"
using namespace indexed;

Status::Status(StatusCode code, const std::string &msg) {
    assert(code != StatusCode::OK);
    state_ = new State;
    state_->code = code;
    state_->msg = msg;
}

void Status::CopyFrom(const Status::State *s) {
    delete state_;
    if (s == nullptr) {
        state_ = nullptr;
    } else {
        state_ = new State(*s);
    }
}

std::string Status::ToString() const {
    std::string result(CodeAsString());
    if (state_ == NULL) {
        return result;
    }
    result += ":";
    result += state_->msg;
    return result;
}

std::string Status::CodeAsString() const {
    if (state_ == NULL) {
        return "OK";
    }
    const char *type;
    switch (code()) {
        case StatusCode::OK:
            type = "OK";
            break;
        case StatusCode::OutOfMemory:
            type = "Out of memory";
            break;
        case StatusCode::KeyError:
            type = "Key error";
            break;
        case StatusCode::TypeError:
            type = "Type error";
            break;
        case StatusCode::Invalid:
            type = "Invalid";
            break;
        case StatusCode::IOError:
            type = "IOError";
            break;
        case StatusCode::UnknownError:
            type = "Unknown error";
            break;
        case StatusCode::NotImplemented:
            type = "NotImplemented";
            break;
        default:
            type = "Unknown";
            break;
    }
    return std::string(type);
}
