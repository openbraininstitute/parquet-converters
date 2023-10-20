/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#ifndef INCLUDE_PROGRESS_HPP_
#define INCLUDE_PROGRESS_HPP_

#include <sys/ioctl.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <stdexcept>


namespace utils {

/**
 * @brief The ProgressMonitor class: Class which monitors progress
 *  and eventually diplays a progressbar. Thread-safe.
 *  API: Instances support next() and += operator to signal a change in progress
 */
class ProgressMonitor {
 public:

    /**
     * @brief ProgressMonitor Generic Constructor of a ProgressMonitor
     * @param total The max count.
     * @param show_bar Displays a progress bar if true.
     *
     * Threading note: All the ctor prams should be the same in all threads.
     */
    ProgressMonitor(size_t total, bool show_bar, int ntasks=1)
        : total_(total)
        , show_bar_(show_bar)
        , done_(0)
        , n_tasks_(ntasks)
    {
        if (show_bar) {
            redraw();
        }
    }

    /**
     * @brief ProgressMonitor Constructor defaulting to display progressbar
     *  if stderr is a TTY
     * @param total The max count (default: 100)
     */
    explicit ProgressMonitor(size_t total = 100)
        : ProgressMonitor(total, isatty(STDERR_FILENO)) {}


    ~ProgressMonitor() {
        if (show_bar_) {
            clear();
        }
    }


    inline ProgressMonitor& operator +=(size_t blocks_done) {
        done_ += blocks_done;   // atomic
        if (show_bar_) {
            redraw();           // thread-safe non-preemtive
        }
        return *this;
    }

    inline void next() {
        (*this) += 1;
    }

    /**
     * @brief set_parallelism: Manually define the number of tasks without
     *  considering subtasks, e.g. for MPI.
     *  When using threaded subtasks you might want to set this to 0.
     */
    void set_parallelism(int n_tasks) {
        n_tasks_ = n_tasks;
    }

    /**
     * @brief Prints some message above the progressbar, thread-safe
     */
    void print_info(const char* format, ...) {
        va_list arglist;
        va_start(arglist, format);
        std::lock_guard<std::mutex> lg(output_mtx_);
        _clear();
        vprintf(format, arglist);
        _show();
        va_end(arglist);
    }

    void redraw()  {
        std::unique_lock<std::mutex> lock(output_mtx_, std::try_to_lock);
        // If a draw is already in progress just skip...
        if (!lock.owns_lock()) {
            return;
        }
        auto end = std::chrono::steady_clock::now();
        if ((end - last_update_).count() < 1) {
            return;  // draw at most once per sec
        }
        last_update_ = std::move(end);
        _show();
    }

    inline void clear()  {
        std::lock_guard<std::mutex> lg(output_mtx_);
        _clear();
    }


 protected:
    static inline unsigned window_cols() {
        struct winsize window;
        unsigned columns = 80;

        if (ioctl(STDERR_FILENO, TIOCGWINSZ, &window) >= 0) {
            if (window.ws_col > 0)
                columns = window.ws_col;
        }
        return columns;
    }

    static const int MAX_BAR_LEN = 80;


 private:

    // Users must call the higher level synchronized methods.

    inline void _clear() {
        fprintf(stderr, "\r%*s\r", last_msg_len_, "");
    }

    inline void _show() {
        static const char* PB_STR = "=================================================="
                                    "==================================================";
        const size_t done = done_.load();
        const int n_tasks = n_tasks_.load();
        const float progress = (total_ == 0)? 1. : float(done) / total_;
        const unsigned cols = window_cols() - 1;

        char tasks_str[30];
        snprintf(tasks_str, 30, "(%ld + %d / %ld)", done, n_tasks, total_);

        int progress_len = cols - (11 + strlen(tasks_str));
        if (progress_len > MAX_BAR_LEN) {
            progress_len = MAX_BAR_LEN;
        }

        const auto bar_len = int(std::round(std::min(1.f, progress) * progress_len));
        int rpad = std::max(0, progress_len - bar_len);
        last_msg_len_ = progress_len + 11 + strlen(tasks_str);

        fprintf(stderr, "\r[%5.1f%%|%.*s>%*s] %s ",
                std::min(progress*100.f, 100.f), bar_len, PB_STR, rpad, "", tasks_str);
    }

    const size_t total_;
    const bool show_bar_;
    std::atomic<size_t> done_;
    std::atomic<int> n_tasks_;

    // output related
    std::mutex output_mtx_;
    std::chrono::time_point<std::chrono::steady_clock> last_update_;
    unsigned last_msg_len_;
};


}  // namespace utils

#endif  // INCLUDE_PROGRESS_HPP_
