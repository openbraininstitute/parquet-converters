#ifndef CONVERTER_H
#define CONVERTER_H

#include "generic_reader.h"
#include "generic_writer.h"
#include "progress.h"


template<typename T>
class Converter {

public:
    ///
    /// \brief The Format enum
    /// RECORDS: data is passed (between reader and writer) in a buffer (array) of the data type
    /// COLUMNS: data is passed as entire blocks defined by DataType, which shall internally manage the buffer
    ///
    enum class Format {RECORDS, COLUMNS};

    Converter(Reader<T> & reader, Writer<T> & writer, Format dataFormat=Format::RECORDS, uint buffer_len=128*1024)
    : _reader(reader),
      _writer(writer)
    {
        n_blocks = _reader.block_count();

        if( dataFormat == Format::RECORDS ) {
            BUFFER_LEN = buffer_len;
            // Default: 128K entries (~5MB)
            _buffer = new T[ (n_blocks>BUFFER_LEN)? BUFFER_LEN : n_blocks ];
        }
        else {
            BUFFER_LEN = 1;
            // we better handle it directly as single object
            _buffer = new T();
        }
    }


    ~Converter() {
        delete _buffer;
    }

    int exportN(unsigned n) {

        if( n > n_blocks ) {
            n = n_blocks;
            printf("Warning: Requested export blocks more than available.\n");
        }

        int n_buffers = n / BUFFER_LEN;
        float progress = .0;
        float progress_inc = 1.0 / (float)n_buffers;
        _reader.seek(0);

        for(int i=0; i<n_buffers; i++) {
            _reader.fillBuffer( _buffer, BUFFER_LEN );
            _writer.write( _buffer, BUFFER_LEN );

            progress +=progress_inc;
            if( progress_handler ) {
                progress_handler(progress);
            }
        }

        // Remaining records, smaller than a buffer
        int remaining = n % BUFFER_LEN;
        _reader.fillBuffer( _buffer, remaining );
        _writer.write( _buffer, remaining );

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

    const unsigned int BUFFER_LEN;

private:
    Format mode;
    T* _buffer;

    Reader<T>& _reader;
    Writer<T>& _writer;
    std::function<void(float)> progress_handler;
    unsigned n_blocks;
};





#endif // CONVERTER_H
