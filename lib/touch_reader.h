#ifndef LOADER_H
#define LOADER_H

#include <fstream>
#include <istream>
#include <functional>
#include <vector>
#include <numeric>
#include <mutex>
#include <string.h>

#include "generic_reader.h"
#include "touch_defs.h"



class TouchReader : public Reader<Touch>{
public:
    TouchReader( const char *filename, bool different_endian=false );
    ~TouchReader();


    Touch & begin();
    Touch & end();

    virtual Touch & getNext() override;

    virtual Touch & getItem( uint index ) override;

    virtual uint fillBuffer(Touch* buf, uint length) override;

    virtual uint record_count() override {
        return _record_count;
    }

    virtual inline void seek(uint pos) override;

    static const uint BUFFER_LEN = 256;

private:
    void _fillBuffer();
    void _load_into( Touch* buf, int length );

    // File
    std::ifstream _touchFile;
    unsigned long _record_count;
    bool _endian_swap=false;

    // Internal Buffer: to be used for iteration
    Touch _buffer[BUFFER_LEN];
    unsigned long _offset;
    uint _it_buf_index;  // Offset relative to buffer
    uint _buffer_record_count;

};



#endif // LOADER_H
