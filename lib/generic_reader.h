#ifndef GENERIC_READER_H
#define GENERIC_READER_H

typedef unsigned int uint;

template <typename T>
class Reader {

public:
    virtual T & getNext() = 0;

    virtual T & getItem( uint index ) = 0;

    // For most operations the small internal buffer is used
    // But in the case of fill buffer the use can specify a much larger region, .e.g. full file conversion
    // NOTE: The buffer must be allocated
    virtual uint fillBuffer(T *buf, uint length) = 0;

    virtual uint record_count() = 0;

    virtual void seek(uint pos) = 0;


protected:
    Reader() { }
    virtual ~Reader();

    // bytes per record
    static constexpr uint RECORD_SIZE = sizeof(T);

};

#endif // GENERIC_READER_H
