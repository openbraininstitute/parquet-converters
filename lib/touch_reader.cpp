#include <time.h>
#include "touch_reader.h"
#include "touch_writer_parquet.h"


static inline void bswap_32(uint32_t* b) {
  /// Some GCCs fail to detect the byte swap
  *b=__builtin_bswap32(*b);
}

using namespace std;


#define swap_int(x) bswap_32((uint32_t*)&x)
#define swap_float(x) bswap_32((uint32_t*)&x)


//More global definitions (lost around the code)
struct NeuronInfoSerialized {
    NeuronInfoSerialized() :
        neuronID(-1),
        touchesCount(0),
        binaryOffset(0)     {}

    int neuronID;
    uint touchesCount;
    long long binaryOffset;
};

struct TouchInfoSerialized {
    TouchInfoSerialized() :
        ids(),
        dis() {}

    int ids[7];
    float dis[3];
};



TouchReader::TouchReader(const char* filename, bool different_endian)
:   _offset(0),
    _it_buf_index(0),
    _buffer_record_count(0)
{
    //test_str_size();
    _touchFile.open(filename, ifstream::binary);
    _touchFile.seekg (0, ifstream::end);
    long int length = _touchFile.tellg();
    _record_count = length / RECORD_SIZE;
    _touchFile.seekg (0);
    _endian_swap = different_endian;

}

TouchReader::~TouchReader() {
    _touchFile.close();
}


Touch & TouchReader::begin() {
    if( _offset > 0 ) {
        _touchFile.seekg (0);
        _fillBuffer();
    }
    return _buffer[0];
}


///
/// \brief Implements a simple iteration protocol.
///        It assumes the buffer is in the correct position
///        If the user wants to change iteration he must call begin() or seek()
///
Touch & TouchReader::getNext() {
    if( _it_buf_index >= _record_count ) {
        // Throw NULL if we are at the end
        throw NULL;
    }

    if(_it_buf_index + 1 >= _buffer_record_count) {
        _fillBuffer();
    }

    return _buffer[_it_buf_index++];
}

///
/// \brief Returns a Touch record from an arbitrary index
///        It will reposition the internal iterator position, so getNext() will restart from the next item
///        Alternativelly use seek() to only define iterator position
///
Touch & TouchReader::getItem(uint index) {
    seek(index);
    return getNext();
}


///
/// \brief TouchReader::seek Changes the file and buffers handlers to a specified position
///        NOTE: This functions doesnt imply any buffer filling.
/// \param pos
///
void TouchReader::seek(uint pos)   {
    if( pos >= _record_count ) {
        throw NULL;
    }

    uint offset = pos / BUFFER_LEN;

    if( offset != _offset ) {
        _offset = offset;
        _touchFile.seekg( _offset*RECORD_SIZE );
        _buffer_record_count = 0;
    }
}


uint TouchReader::fillBuffer( Touch* buf, uint load_n ) {
    if( load_n + _offset > _record_count ) {
        load_n = _record_count - _offset;
    }

    _load_into( buf, load_n );
    return load_n;
}


void TouchReader::_fillBuffer() {
    uint load_n = BUFFER_LEN;
    if( load_n + _offset > _record_count ) {
        load_n = _record_count - _offset;
    }

    _load_into(_buffer, load_n);
    _offset += load_n;
    _buffer_record_count = load_n;
}


void TouchReader::_load_into( Touch* buf, int length ) {
    _touchFile.read( (char*)buf, length*RECORD_SIZE );

    if( _endian_swap ){
        Touch* curTouch = buf;
        for(int i=0; i<length; i++) {
            // Given all the fields are contiguous and are 32bits long
            // we loop over them as if it was an array
            uint32_t* touch_data = (uint32_t*) (curTouch);
            for(int j=0; j<10; j++) {
                bswap_32(touch_data+j);
            }
            // next
            curTouch++;
        }
    }
}


