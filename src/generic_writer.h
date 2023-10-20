/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#pragma once

#include <cstdint>

template <typename T>
class Writer {
 public:
    virtual void setup(const typename T::Schema*, std::shared_ptr<const typename T::Metadata>) = 0;
    virtual void write(const T* data, uint32_t length) = 0;

    // bytes per record
    static constexpr uint32_t RECORD_SIZE = sizeof(T);

 protected:
    // prevent polymorphic desctruction
    ~Writer() = default;
};
