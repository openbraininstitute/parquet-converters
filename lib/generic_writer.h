#ifndef GENERIC_WRITER_H
#define GENERIC_WRITER_H

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

#endif // GENERIC_WRITER_H
