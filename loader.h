#ifndef LOADER_H
#define LOADER_H

#include <fstream>
#include <istream>

//Block is 40bytes, Buffer len should also be multiple of 40
#define BUFFER_LEN 10*1024

//forward def
class ParquetWriter;

//Int array
enum TOUCH_LOCATION { NEURON_ID, SECTION_ID, SEGMENT_ID };

struct Touch {
    int getPreNeuronID(){
        return pre_synapse_ids[NEURON_ID];
    }
    int getPostNeuronID(){
        return post_synapse_ids[NEURON_ID];
    }

    int pre_synapse_ids[3];
    int post_synapse_ids[3];
    int branch;
    float distance_soma;
    float pre_offset, post_offset;
};


class Loader {
public:
    Loader( const char *filename );
    ~Loader( );
    Touch & getNext();
    Touch & getItem( int index );
    void setExporter( ParquetWriter &writer);
    int exportN( int n );
    int exportAll( );

    static const int TOUCH_BUFFER_LEN = BUFFER_LEN;
    static const int BLOCK_SIZE = sizeof(Touch);


private:
    int cur_buffer_base;
    int cur_it_index;
    ParquetWriter* mwriter ;
    Touch touches_buf[TOUCH_BUFFER_LEN];
    std::ifstream touchFile;
    long int n_blocks;

    void _fillBuffer();

};

#endif // LOADER_H
