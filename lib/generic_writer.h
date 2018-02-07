#ifndef GENERIC_WRITER_H
#define GENERIC_WRITER_H

typedef unsigned int uint;

template <typename T>
class Writer {

public:
    virtual void write(T* data, uint length) = 0;

    // bytes per record
    static constexpr uint RECORD_SIZE = sizeof(T);
};

#endif // GENERIC_WRITER_H
