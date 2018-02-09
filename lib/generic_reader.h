#ifndef GENERIC_READER_H
#define GENERIC_READER_H

typedef unsigned int uint;

template <typename T>
class Reader {

public:
    // For most operations the small internal buffer is used
    // But in the case of fill buffer the use can specify a much larger region, .e.g. full file conversion
    // NOTE: The buffer must be allocated
    virtual uint fillBuffer(T *buf, uint length) = 0;

    virtual uint block_count() = 0;

    virtual void seek(uint pos) = 0;

    // bytes per record
    static constexpr uint RECORD_SIZE = sizeof(T);

protected:
    ~Reader() = default;

};

#endif // GENERIC_READER_H
