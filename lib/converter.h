#ifndef CONVERTER_H
#define CONVERTER_H

#include "generic_reader.h"
#include "generic_writer.h"
#include "progress.h"

namespace neuron_parquet {

///
/// \brief The Format enum
/// RECORDS: data is passed (between reader and writer) in a buffer (array) of the data type
/// COLUMNS: data is passed as entire blocks defined by DataType, which shall internally manage the buffer
///
enum class ConverterFormat {RECORDS, COLUMNS};

template<typename T>
class Converter {

public:

    Converter(Reader<T> & reader, Writer<T> & writer, ConverterFormat dataFormat=ConverterFormat::RECORDS, uint buffer_len=128*1024)
    : _reader(reader),
      _writer(writer),
      // With blocks the buffer is virtual, therefore len 1
      BUFFER_LEN((dataFormat == ConverterFormat::RECORDS)? buffer_len : 1 )
    {
        n_blocks = _reader.block_count();

        if( dataFormat == ConverterFormat::RECORDS ) {
            // Default: 128K entries (~5MB)
            _buffer = new T[ (n_blocks>BUFFER_LEN)? BUFFER_LEN : n_blocks ];
        }
        else {
            // we better handle it directly as single object
            _buffer = new T();
        }
    }


    int exportN(unsigned n) {

        if( n > n_blocks ) {
            n = n_blocks;
            printf("Warning: Requested export blocks more than available.\n");
        }

        int n_buffers = n / BUFFER_LEN;
        float progress = .0;
        float progress_inc;
        if( n_buffers > 0 ) {
            progress_inc = 1.0 / (float)n_buffers;
        }

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


private:
    ConverterFormat mode;
    Reader<T>& _reader;
    Writer<T>& _writer;
    const unsigned int BUFFER_LEN;
    T* _buffer;

    std::function<void(float)> progress_handler;
    unsigned n_blocks;
};



} //NS

#endif // CONVERTER_H
