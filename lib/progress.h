#ifndef PROGRESS_HANDLER_H
#define PROGRESS_HANDLER_H

#include <vector>
#include <mutex>
#include <functional>
#include <atomic>

/// A thread-safe ProgressBar
class ProgressMonitor {
public:
    ProgressMonitor(int n_tasks=1);

    inline void showProgress();

    inline void updateProgress(float progress, int task_i);

    void task_done(int task_i);

    /// Returns a handler function for a new thread
    std::function<void(float)> getNewHandler();

private:
    //Number of tasks. By default is 1.
    int n_tasks;
    std::atomic_int tasks_active;
    std::atomic_int tasks_done;

    std::vector<float> progressV;
    float global_progress = .0;

    std::mutex _progress_mtx;

};

#endif // PROGRESS_HANDLER_H
