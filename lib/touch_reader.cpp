#include <time.h>
#include "touch_reader.h"
#include "touch_writer_parquet.h"


static const unsigned TOUCH_BUFFER_LEN = 4096;

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
    unsigned int touchesCount;
    long long binaryOffset;
};

struct TouchInfoSerialized {
    TouchInfoSerialized() :
        ids(),
        dis() {}

    int ids[7];
    float dis[3];
};



TouchReader::TouchReader(const char* filename, bool different_endian):
    cur_buffer_base(0),
    cur_it_index(0)
{
    //test_str_size();
    touchFile.open(filename, ifstream::binary);
    touchFile.seekg (0, ifstream::end);
    long int _length = touchFile.tellg();
    n_blocks = _length / BLOCK_SIZE;
    touchFile.seekg (0, ifstream::beg);
    endian_swap = different_endian;

    _fillBuffer();
}

TouchReader::~TouchReader() {
    touchFile.close();
}

Touch & TouchReader::getNext() {
    if( cur_it_index+1 >= n_blocks ) {
        throw NULL;
    }
    int relative_index = cur_it_index - cur_buffer_base;

    if(relative_index +1 >= TOUCH_BUFFER_LEN) {
        cur_buffer_base += TOUCH_BUFFER_LEN;
        relative_index=0;

        touchFile.seekg( cur_buffer_base*BLOCK_SIZE, ifstream::beg );
        _fillBuffer();
    }
    cur_it_index++;

    return touches_buf[relative_index];
}


Touch & TouchReader::getItem(int index) {
    if( index >= n_blocks ) {
        throw NULL;
    }
    int relative_index = index % TOUCH_BUFFER_LEN;
    int base = index-relative_index ;

    if(base != cur_buffer_base) {
        cur_buffer_base = base;
        touchFile.seekg( cur_buffer_base*BLOCK_SIZE, ifstream::beg );
        _fillBuffer();
    }
    return touches_buf[relative_index];

}


void TouchReader::_load_into( Touch* buf, int length ) {
    touchFile.read((char*)buf, length*BLOCK_SIZE );

    if( endian_swap ){
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


void TouchReader::_fillBuffer() {
    if(!touchFile.is_open()) {
        printf("File not open");
        return ;
    }

    //Assume the file pointer is well positioned
    //two options.. we can read BUF_LENGTH from file, or not enough file to read
    int load_n = n_blocks-cur_buffer_base;
    if( load_n > TOUCH_BUFFER_LEN ) load_n=TOUCH_BUFFER_LEN;

    _load_into( touches_buf, load_n );

}




