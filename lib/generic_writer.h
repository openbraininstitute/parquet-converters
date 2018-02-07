#ifndef GENERIC_WRITER_H
#define GENERIC_WRITER_H

typedef unsigned int uint;

template <typename T>
class Writer {

public:
    virtual void write(T* data, uint length) = 0;

    // bytes per record
    static constexpr uint RECORD_SIZE = sizeof(T);

protected:
    // prevent polymorphic desctruction
    ~Writer() = default;
};

#endif // GENERIC_WRITER_H
