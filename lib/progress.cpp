#include "progress.h"
#include <string.h>
#include <numeric>


static const char* const PB_STR = "================================================================================";


ProgressMonitor::ProgressMonitor(int n_tasks)
    : n_tasks(n_tasks),
      tasks_active(0),
      tasks_done(0)
    {}

void ProgressMonitor::showProgress() {
    static const int PB_LEN = strlen(PB_STR);
    int bar_len = (int) (global_progress * PB_LEN);
    int rpad = PB_LEN - bar_len;
    fprintf(stderr, "\r[%5.1f%%|%.*s>%*s] (%d + %d / %d)",
            global_progress*100, bar_len, PB_STR, rpad, "", tasks_done.load(), tasks_active.load(), n_tasks);
}

void ProgressMonitor::updateProgress(float progress, int task_i) {

    if( progress-progressV[task_i] < 0.001 ) {
        return;
    }
    
    if (progressV[task_i] < 0.00001) {
        tasks_active ++;
    }

    // If a thread made some visible progress he is elegible for recalculating the global average
    // which might update the visual representation
    // If another thread is already doing so we skip
    
    progressV[task_i]=progress;

    if(_progress_mtx.try_lock()) {
        float average = std::accumulate(progressV.begin(), progressV.end(), 0.0) / n_tasks;
        if( average > global_progress + 0.0005 ){
            global_progress = average;
            showProgress();
        }
        _progress_mtx.unlock();
    }
}

void ProgressMonitor::task_done(int task_i) {
    tasks_done ++;
    tasks_active --;
    if(_progress_mtx.try_lock()) {
        showProgress();
        _progress_mtx.unlock();
    }
}

std::function<void(float)> ProgressMonitor::getNewHandler(){
    int idx = progressV.size();
    progressV.push_back(.0);
    return [this, idx](float progress){ this->updateProgress(progress, idx); };
}
