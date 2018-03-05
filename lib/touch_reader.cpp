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
    uint32_t touchesCount;
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
:   offset_(0),
    it_buf_index_(0),
    buffer_record_count_(0)
{
    //test_str_size();
    touchFile_.open(filename, ifstream::binary);
    touchFile_.seekg (0, ifstream::end);
    uint64_t length = touchFile_.tellg();
    record_count_ = length / RECORD_SIZE;
    touchFile_.seekg (0);
    endian_swap_ = different_endian;

}

TouchReader::~TouchReader() {
    touchFile_.close();
}


Touch & TouchReader::begin() {
    return getItem(0);
}


///
/// \brief Implements a simple iteration protocol.
///        It assumes the buffer is in the correct position
///        If the user wants to change iteration he must call begin() or seek()
///
Touch & TouchReader::getNext() {
    uint32_t next_position = it_buf_index_ + 1;

    if( offset_ + next_position >= record_count_ ) {
        // Throw NULL if we are at the end
        throw NULL;
    }

    if(next_position >= buffer_record_count_) {
        // Need change buffer offset?
        if(next_position >= BUFFER_LEN) {
            seek(offset_ + next_position);
        }
        _fillBuffer();
    }

    return buffer_[it_buf_index_++];
}

///
/// \brief Returns a Touch record from an arbitrary index
///        It will reposition the internal iterator position, so getNext() will restart from the next item
///        Alternativelly use seek() to only define iterator position
///
Touch & TouchReader::getItem(uint32_t index) {
    seek(index);
    return getNext();
}


///
/// \brief TouchReader::seek Changes the file and buffers handlers
///        So that the specified position is contained in the buffer
///        NOTE: This functions doesnt imply any buffer filling.
/// \param pos
///
void TouchReader::seek(uint64_t pos)   {
    if( pos >= record_count_ ) {
        throw runtime_error(string("Invalid file position ") + to_string(pos));
    }

    uint32_t nth_buffer = (pos / BUFFER_LEN);
    uint32_t new_offset = nth_buffer * BUFFER_LEN;

    if( new_offset != offset_ ) {
        offset_ = new_offset;
        touchFile_.seekg( new_offset*RECORD_SIZE );
        buffer_record_count_ = 0;
    }
    it_buf_index_ = pos - new_offset;
}


/**
 * @brief TouchReader::fillBuffer Fills a given buffer, incrementing the offset
 *        NOTE: This invalidates the internal buffer. A seek shall be performed before changing buffers
 * @return The number of records read
 */
uint32_t TouchReader::fillBuffer( Touch* buf, uint32_t load_n ) {
    if( load_n + offset_ > record_count_ ) {
        load_n = record_count_ - offset_;
    }

    _load_into( buf, load_n );
    offset_ += load_n;
    return load_n;
}


void TouchReader::_fillBuffer() {
    uint32_t load_n = BUFFER_LEN;
    if( load_n + offset_ > record_count_ ) {
        load_n = record_count_ - offset_;
    }

    _load_into(buffer_, load_n);
    offset_ += load_n;
    buffer_record_count_ = load_n;
}


void TouchReader::_load_into( Touch* buf, uint32_t length ) {
    touchFile_.read( (char*)buf, length*RECORD_SIZE );

    if( endian_swap_ ){
        Touch* curTouch = buf;
        for(uint32_t i=0; i<length; i++) {
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

