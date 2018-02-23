#ifndef PROGRESS_HANDLER_H
#define PROGRESS_HANDLER_H

#include <vector>
#include <mutex>
#include <functional>
#include <atomic>

/// A thread-safe ProgressBar
class ProgressMonitor {
public:
    ProgressMonitor(size_t n_blocks, int n_handlers=1, bool display_bar=true);

    inline void showProgress();

    inline void updateProgress(int tasks_done_inc);

    void task_start(int count=1);
    void task_done(int count=1);

    /// Returns a handler function for a new thread
    std::function<void(int)> getNewHandler();

    float cur_progress() const {
        return global_progress_;
    }

private:
    const uint n_blocks_;
    //Number of tasks. By default is 1.
    int n_handlers_;
    const bool display_bar_;
    std::atomic_int tasks_active_;
    std::atomic_int blocks_done_;
    float global_progress_;

    std::mutex progress_mtx_;
    std::mutex handler_mtx_;

};

#endif // PROGRESS_HANDLER_H
