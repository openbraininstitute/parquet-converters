#ifndef GENERIC_READER_H
#define GENERIC_READER_H


template <typename T>
class Reader {

public:
    virtual T & getNext();

    virtual T & getItem( int index );

    // For most operations the small internal buffer is used
    // But in the case of fill buffer the use can specify a much larger region, .e.g. full file conversion
    // NOTE: The buffer must be allocated
    virtual unsigned fillBuffer(T *buf, unsigned length);

    virtual unsigned record_count() ;

    virtual unsigned seek(unsigned pos) ;


protected:
    Reader() { }
    virtual ~Reader();

    // bytes per record
    constexpr static unsigned RECORD_SIZE = sizeof(T);
};

#endif // GENERIC_READER_H
