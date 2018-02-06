#ifndef GENERIC_WRITER_H
#define GENERIC_WRITER_H

template <typename T>
class Writer {

public:
    virtual void write(T* data, int length) ;

};

#endif // GENERIC_WRITER_H
