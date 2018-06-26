#ifndef GENERIC_READER_H
#define GENERIC_READER_H

#include <cstdint>

template <typename T>
class Reader {

public:
    // For most operations the small internal buffer is used
    // But in the case of fill buffer the use can specify a much larger region, .e.g. full file conversion
    // NOTE: The buffer must be allocated
    virtual uint32_t fillBuffer(T *buf, uint32_t length) = 0;

    virtual uint64_t record_count() const = 0;

    // Blocks are chunks of records that are read together.
    // Non-chunked formats shall return 0
    virtual uint32_t block_count() const = 0;

    virtual void seek(uint64_t pos) = 0;

    virtual bool is_chunked() const = 0;

    // bytes per record
    static constexpr uint32_t RECORD_SIZE = sizeof(T);

protected:
    ~Reader() = default;

};

#endif // GENERIC_READER_H
