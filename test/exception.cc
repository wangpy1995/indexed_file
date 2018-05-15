//
// Created by wangpengyu6 on 18-4-28.
//

#include "exception.h"
#include <sstream>

namespace indexed {
    void IndexedException::EofException(const std::string &msg) {
        std::stringstream ss;
        ss << "Unexpected end of stream.";
        if (!msg.empty()) {
            ss << ":" << msg;
        }
        throw IndexedException(ss.str());
    }

    void IndexedException::NYI(const std::string &msg) {
        std::stringstream ss;
        ss << "Not yet implemented: " << msg << ".";
        throw IndexedException(ss.str());
    }

    void IndexedException::Throw(const std::string &msg) {
        throw IndexedException(msg);
    }

    IndexedException::IndexedException(const char *msg) : msg_(msg) {}

    IndexedException::IndexedException(const std::string &msg) : msg_(msg) {}

    IndexedException::IndexedException(const std::string &msg, std::exception &e) : msg_(msg) {}

    IndexedException::~IndexedException() throw() {}

    const char *indexed::IndexedException::what() const throw(){
        return msg_.c_str();
    }
}