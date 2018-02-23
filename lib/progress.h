#ifndef PROGRESS_HANDLER_H
#define PROGRESS_HANDLER_H

#include <vector>
#include <mutex>
#include <functional>
#include <atomic>

/// A thread-safe ProgressBar
class ProgressMonitor {
public:
    ProgressMonitor(int n_tasks=1, bool display_bar=true);

    inline void showProgress();

    inline void updateProgress(float progress, int task_i);

    void task_done(int task_i);

    /// Returns a handler function for a new thread
    std::function<void(float)> getNewHandler();

    float cur_progress() const {
        return global_progress;
    }

private:
    //Number of tasks. By default is 1.
    int n_tasks;
    const bool display_bar;
    std::atomic_int tasks_active;
    std::atomic_int tasks_done;

    std::vector<float> progressV;
    float global_progress;

    std::mutex progress_mtx_;
    std::mutex handler_mtx_;

};

#endif // PROGRESS_HANDLER_H
