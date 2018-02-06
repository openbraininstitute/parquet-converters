#ifndef CONVERTER_H
#define CONVERTER_H

#include "generic_reader.h"
#include "generic_writer.h"
#include "progress.h"


template<typename T, int BUFFER_LEN=4096>
class Converter {

public:
    Converter( Reader<T> & reader, Writer<T> & writer )
    : _reader(reader),
      _writer(writer)
    {
        n_blocks = _reader.record_count();
    }

    int exportN(int n) {
        int n_buffers= n / BUFFER_LEN;
        float progress = .0;
        float progress_inc = 1.0 / (float)n_buffers;
        time_t time_sec = time(0);
        _reader.seek(0);

        for(int i=0; i<n_buffers; i++) {
            _reader.fillBuffer( &buffer, BUFFER_LEN );
            _writer.write( &buffer, BUFFER_LEN );

            progress +=progress_inc;
            if( progress_inc > 0.1 || time(0) > time_sec ){
                if( progress_handler ) {
                    progress_handler(progress);
                }
                time_sec=time(0);
            }
        }

        // Remaining records, smaller than a buffer
        int remaining = n % BUFFER_LEN;
        _reader.fillBuffer( &buffer, remaining );
        _writer.write( &buffer, remaining );

        if( progress_handler ) {
            progress_handler(1.0);
        }

        return n;
    }

    int exportAll() {
        exportN(n_blocks);
        return n_blocks;
    }


    void setProgressHandler(std::function<void(float)> func) {
        progress_handler = func;
    }


private:
    T buffer[BUFFER_LEN];

    Reader<T>& _reader;
    Writer<T>& _writer;
    std::function<void(float)> progress_handler;
    unsigned n_blocks;
};





#endif // CONVERTER_H
