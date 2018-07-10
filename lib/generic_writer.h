/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#ifndef LIB_GENERIC_WRITER_H_
#define LIB_GENERIC_WRITER_H_

#include <cstdint>

template <typename T>
class Writer {
 public:
    virtual void write(const T* data, uint32_t length) = 0;

    // bytes per record
    static constexpr uint32_t RECORD_SIZE = sizeof(T);

 protected:
    // prevent polymorphic desctruction
    ~Writer() = default;
};

#endif  // LIB_GENERIC_WRITER_H_
