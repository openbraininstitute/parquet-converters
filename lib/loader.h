#ifndef LOADER_H
#define LOADER_H

#include <fstream>
#include <istream>
#include <functional>
#include <vector>
#include <numeric>

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
    Loader( const char *filename, bool different_endian=false );
    ~Loader( );
    Touch & getNext();
    Touch & getItem( int index );
    void setExporter( ParquetWriter &writer);
    int exportN( int n );
    int exportAll( );
    void setProgressHandler(std::function<void(float)> func);

    static const int TOUCH_BUFFER_LEN = BUFFER_LEN;
    static const int BLOCK_SIZE = sizeof(Touch);


private:
    int cur_buffer_base;
    int cur_it_index;
    ParquetWriter* mwriter ;
    Touch touches_buf[TOUCH_BUFFER_LEN];
    std::ifstream touchFile;
    long int n_blocks;
    bool endian_swap=false;
    std::function<void(float)> progress_handler;

    void _fillBuffer();
    void _load_into( Touch* buf, int length );

};


class ProgressHandler {
private:
    //Multitask version
    int n_tasks;
    std::vector<float> progressV;
    float global_progress = .0;
    float last_large_progress = .0;

public:
    ProgressHandler(int n):
        n_tasks(n)   {
    }

    void showProgress(float progress){
        fprintf(stderr, "%2.1f%%\n", global_progress*100);
    }

    void showSmallProgress() {
        fprintf(stderr, ".");
    }

    void updateGlobalProgress( float progress ) {
        showSmallProgress();
        if( global_progress > last_large_progress + 0.02 ) {
            last_large_progress = global_progress;
            showProgress(global_progress);
        }
    }

    void updateProgress(float progress, int task_i){
        progressV[task_i]=progress;
        float average = accumulate( progressV.begin(), progressV.end(), 0.0)/(float)n_tasks;
        if( average > global_progress + 0.001 ){
            global_progress = average;
            updateGlobalProgress(global_progress);
        }
    }

    std::function<void(float)> addSubTask(){
        int size=progressV.size();
        progressV.push_back(.0);
        return [this, size](float progress){ this->updateProgress(progress, size); };
    }

};



#endif // LOADER_H
