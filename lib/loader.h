#ifndef LOADER_H
#define LOADER_H

#include <fstream>
#include <istream>
#include <functional>
#include <vector>
#include <numeric>
#include <mutex>
#include <string.h>


//Block is 40bytes, Buffer len should also be multiple of 40
#define BUFFER_LEN 64*1024

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


#define PB_STR "================================================================================"

class ProgressHandler {
public:

    ProgressHandler(int n):
        n_tasks(n)   {
    }

    inline void showProgress(float progress, int tasks_done){
        static const int PB_LEN = strlen(PB_STR);
        int bar_len = (int) (progress * PB_LEN);
        int rpad = PB_LEN - bar_len;
        fprintf(stderr, "\r[%5.1f%%|%.*s>%*s] (%d + %d / %d)",
                progress*100, bar_len, PB_STR, rpad, "", tasks_done, tasks_active, n_tasks);
    }

    void updateProgress(float progress, int task_i){
        bool shall_update_progress = false;

        if (progress > 0.9999) {
            tasks_done ++;
            tasks_active --;
        }
        else {
            if (progressV[task_i] == .0) {
                tasks_active ++;
            }
        }

        progressV[task_i]=progress;
        float average = std::accumulate(progressV.begin(), progressV.end(), 0.0) / n_tasks;

        _progress_mtx.lock();
        if( average > global_progress + 0.0005 ){
            global_progress = average;
            shall_update_progress = true;
        }
        _progress_mtx.unlock();

        if(shall_update_progress) {
            showProgress(global_progress, tasks_done);
        }
    }

    std::function<void(float)> addSubTask(){
        int size=progressV.size();
        progressV.push_back(.0);
        return [this, size](float progress){ this->updateProgress(progress, size); };
    }

private:
    //Multitask version
    int n_tasks;
    int tasks_active = 0;
    int tasks_done = 0;

    std::vector<float> progressV;
    float global_progress = .0;
    float last_large_progress = .0;

    std::mutex _progress_mtx;

};



#endif // LOADER_H
