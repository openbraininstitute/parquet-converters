#ifndef PROGRESS_HANDLER_H
#define PROGRESS_HANDLER_H


/// A thread-safe ProgressBar
class ProgressMonitor {
public:
    ProgressMonitor();

    inline void showProgress(float progress, int tasks_done);

    void updateProgress(float progress, int task_i);

    /// Returns a handler function for a new thread
    std::function<void(float)> getNewHandler();

private:
    //Number of tasks. By default is 1.
    int n_tasks;
    int tasks_active = 0;
    int tasks_done = 0;

    std::vector<float> progressV;
    float global_progress = .0;
    float last_large_progress = .0;

    std::mutex _progress_mtx;

};

#endif // PROGRESS_HANDLER_H
