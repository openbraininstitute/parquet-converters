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



class TouchReader : Reader<Touch>{
public:
    TouchReader( const char *filename, bool different_endian=false );
    ~TouchReader( );

    inline Touch & getNext();

    Touch & getItem( int index );

    inline unsigned fillBuffer(Touch* buf, unsigned length);

    inline unsigned record_count();

    inline void seek(int pos) {
        touchFile.seekg (pos * RECORD_SIZE);
    }


private:

    std::ifstream touchFile;
    long int n_blocks;

    bool endian_swap=false;

    void _fillBuffer();
    void _load_into( Touch* buf, int length );

};



#endif // LOADER_H
