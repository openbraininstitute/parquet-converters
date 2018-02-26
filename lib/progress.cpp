#include "progress.h"
#include <string.h>
#include <numeric>


static const char* const PB_STR = "======================================================================";


ProgressMonitor::ProgressMonitor(size_t n_blocks, bool display_bar)
    : n_blocks_(n_blocks),
      display_bar_(display_bar),
      tasks_active_(0),
      blocks_done_(0),
      global_progress_(.0)
    {}

void ProgressMonitor::showProgress() {
    static const int PB_LEN = strlen(PB_STR);
    int bar_len = (int) (global_progress_ * PB_LEN);
    int rpad = PB_LEN - bar_len;
    fprintf(stderr, "\r[%5.1f%%|%.*s>%*s] (%d + %d / %d)",
            global_progress_*100, bar_len, PB_STR, rpad, "", blocks_done_.load(), tasks_active_.load(), n_blocks_);
}

void ProgressMonitor::updateProgress(int blocks_done_inc) {

    blocks_done_ += blocks_done_inc;

    // After update the thread is elegible for recalculating the global average
    // which might update the visual representation
    // If another thread is already doing so we skip

    if(progress_mtx_.try_lock()) {
        float average = (float)blocks_done_ / n_blocks_;
        if( average > global_progress_ + 0.0005 ){
            global_progress_ = average;
            if( display_bar_ )  {
                showProgress();
            }
        }
        progress_mtx_.unlock();
    }
}

void ProgressMonitor::next() {
    updateProgress(1);
}

void ProgressMonitor::task_start(int count) {
    task_done(-count);
}

void ProgressMonitor::task_done(int count) {
    tasks_active_ -= count;

    if(display_bar_) {
        if( progress_mtx_.try_lock() ) {
            showProgress();
            progress_mtx_.unlock();
        }
    }
}

std::function<void(int)> ProgressMonitor::getNewHandler(){
    return [this](int blocks_done){
        this->updateProgress(blocks_done);
    };
}
