/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#ifndef LIB_GENERIC_READER_H_
#define LIB_GENERIC_READER_H_

#include <cstdint>

template <typename T>
class Reader {
 public:
    // For most operations the small internal buffer is used
    // But in the case of fill buffer the use can specify a much larger region,
    //  .e.g. full file conversion
    // NOTE: The buffer must be allocated
    virtual uint32_t fillBuffer(T *buf, uint32_t length) = 0;

    virtual uint64_t record_count() const = 0;

    // Blocks are chunks of records that are read together.
    // Non-chunked formats shall return 0
    virtual uint32_t block_count() const = 0;

    virtual void seek(uint64_t pos) = 0;

    virtual bool is_chunked() const = 0;

    virtual const typename T::Schema* schema() const = 0;
    virtual const std::shared_ptr<const typename T::Metadata> metadata() const = 0;

 protected:
    ~Reader() = default;
};

#endif  // LIB_GENERIC_READER_H_
