#include "progress.h"
#include <string.h>
#include <numeric>


static const char* const PB_STR = "================================================================================";


ProgressMonitor::ProgressMonitor(int n_tasks)
    : n_tasks(n_tasks)
    {}

inline void ProgressMonitor::showProgress(float progress, int tasks_done) {
    static const int PB_LEN = strlen(PB_STR);
    int bar_len = (int) (progress * PB_LEN);
    int rpad = PB_LEN - bar_len;
    fprintf(stderr, "\r[%5.1f%%|%.*s>%*s] (%d + %d / %d)",
            progress*100, bar_len, PB_STR, rpad, "", tasks_done, tasks_active, n_tasks);
}

void ProgressMonitor::updateProgress(float progress, int task_i) {
    bool shall_update_progress = false;

    if (progress > 0.99999) {
        tasks_done ++;
        tasks_active --;
    }
    else {
        if (progressV[task_i] < 0.00001) {
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

std::function<void(float)> ProgressMonitor::getNewHandler(){
    int idx = progressV.size();
    progressV.push_back(.0);
    return [this, idx](float progress){ this->updateProgress(progress, idx); };
}
