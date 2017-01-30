#include "loader.h"
#include "parquetwriter.h"
#include <time.h>

using namespace std;

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


static void test_str_size() {
    printf( "Size of TouchInfoSerialized:%lu\n", sizeof(TouchInfoSerialized) );
    printf( "Size of Touch structa:%lu\n", sizeof(Touch) );
}


Loader::Loader(const char* filename):
    cur_buffer_base(0),
    cur_it_index(0)
{
    test_str_size();
    touchFile.open(filename, ifstream::binary);
    touchFile.seekg (0, ifstream::end);
    long int _length = touchFile.tellg();
    n_blocks = _length / BLOCK_SIZE;
    touchFile.seekg (0, ifstream::beg);

    _fillBuffer();
}

Loader::~Loader() {
    touchFile.close();
}

Touch & Loader::getNext() {
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


Touch & Loader::getItem(int index) {
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


void Loader::_fillBuffer() {
    if(!touchFile.is_open()) {
        printf("File not open");
        return ;
    }

    //Assume the file pointer is well positioned
    //two options.. we can read BUF_LENGTH from file, or not enough file to read
    int load_n = n_blocks-cur_buffer_base;
    if( load_n > TOUCH_BUFFER_LEN ) load_n=TOUCH_BUFFER_LEN;

    touchFile.read((char*)&touches_buf, load_n*BLOCK_SIZE );
}

void Loader::setExporter(ParquetWriter &writer) {
    mwriter = &writer;
}

int Loader::exportN(int n) {
    int n_buffers= n / TOUCH_BUFFER_LEN;
    float progress = .0;
    float progress_inc = 1.0 / (float)n_buffers;
    time_t time_sec = time(0);
    touchFile.seekg (0, ifstream::beg);

    for(int i=0; i<n_buffers; i++) {
        touchFile.read((char*)&touches_buf, TOUCH_BUFFER_LEN*BLOCK_SIZE );
        mwriter->write( touches_buf, TOUCH_BUFFER_LEN );
        progress +=progress_inc;
        if( time(0) > time_sec ){
            time_sec=time(0);
            printf("%2.1f%%\n", progress*100);
            fflush(stdout);
        }
    }

    int remaining = n % TOUCH_BUFFER_LEN;
    touchFile.read((char*)&touches_buf, remaining*BLOCK_SIZE );
    mwriter->write( touches_buf, remaining );

    return n;
}

int Loader::exportAll() {
    exportN(n_blocks);
    return n_blocks;
}




