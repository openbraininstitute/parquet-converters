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
#include <atomic>
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <thread>


namespace utils {

/**
 * @brief The ProgressMonitor class: Class which monitors progress
 *  and eventually diplays a progressbar. Suports multithreading.
 *  API: Instances support next() and += operator to signal a change in progress
 *  In order to property track the number of execution units (e.g. threads) one can
 *  invoke subtask() so the returned object descreses task count upon destruction.
 */
class ProgressMonitor {
 public:
    class SubTask;

    /**
     * @brief ProgressMonitor Generic Constructor of a ProgressMonitor
     * @param total The max count.
     * @param show_bar Displays a progress bar if true
     */
    ProgressMonitor(size_t total, bool show_bar)
        : total_(total)
        , done_(0)
        , n_tasks_(1)
        , last_msg_len_(0)
        , show_bar_(show_bar)
        , must_redraw_(true)
    {
        if (total <= 0)
            throw std::runtime_error("Total blocks cant be zero");
        if (show_bar)
            _init_timer();
    }


    /**
     * @brief ProgressMonitor Constructor defaulting to display progressbar
     *  if stderr is a TTY
     * @param total The max count (default: 100)
     */
    explicit ProgressMonitor(size_t total = 100)
        : ProgressMonitor(total, isatty(STDERR_FILENO)) {}


    ~ProgressMonitor() {
        if (!show_bar_)
            return;
        show_bar_ = false;
        must_redraw_ = false;
        clear();
    }


    inline ProgressMonitor& operator +=(int blocks_done) {
        done_+= blocks_done;
        must_redraw_ = true;
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

    void add_task() {
        n_tasks_++;
        must_redraw_ = true;
    }

    void del_task() {
        n_tasks_--;
        must_redraw_ = true;
    }

    SubTask subtask() {
        return SubTask(*this);
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
        must_redraw_ = true;
        va_end(arglist);
    }


    inline void clear()  {
        std::lock_guard<std::mutex> lg(output_mtx_);
        _clear();
    }

    /**
     * @brief The SubTask class
     *  calling subtask() on a Progress monitor will return a SubTask object
     *  which increases/decreses the task count automatically
     */
    class SubTask {
      friend class ProgressMonitor;
     public:
        ~SubTask() {
            pm_.del_task();
        }
        inline SubTask& operator +=(int n) {
            pm_+=n;
            return *this;
        }

     protected:
        explicit SubTask(ProgressMonitor& pm)
            : pm_(pm) {
            pm.add_task();
        }

     private:
        ProgressMonitor& pm_;
    };


 protected:
    static inline int window_cols() {
        struct winsize window;
        int columns = 80;

        if (ioctl(STDERR_FILENO, TIOCGWINSZ, &window) >= 0) {
            if (window.ws_col > 0)
                columns = window.ws_col;
        }
        return columns;
    }

    static const int MAX_BAR_LEN = 80;


 private:
    inline void _clear() {
        fprintf(stderr, "\r%*s\r", last_msg_len_, "");
    }

    inline void _show() {
        static const char* PB_STR = "=================================================="
                                    "==================================================";
        // If smtg is messing with output already, skip
        if (!output_mtx_.try_lock())
            return;

        // copy values and immediately set false so threads can set true during execution
        size_t done = done_.load();
        int n_tasks = n_tasks_.load();
        must_redraw_ = false;

        float progress = (float)done / total_;
        int cols = window_cols()-1;

        char tasks_str[30];
        snprintf(tasks_str, 30, "(%ld + %d / %ld)", done, n_tasks, total_);

        int progress_len = cols - (11 + strlen(tasks_str));
        if (progress_len > MAX_BAR_LEN) {
            progress_len = MAX_BAR_LEN;
        }

        int bar_len = (progress * progress_len);
        int rpad = progress_len - bar_len;
        last_msg_len_ = progress_len + 11 + strlen(tasks_str);

        fprintf(stderr, "\r[%5.1f%%|%.*s>%*s] %s ",
                progress*100, bar_len, PB_STR, rpad, "", tasks_str);

        output_mtx_.unlock();
    }

    /**
     * @brief Timer
     * Checks every 100ms and redraws the progress bar when there's new data
     */
    void _init_timer() {
        using std::chrono::milliseconds;
        std::thread([this](){
            while (show_bar_) {
                std::this_thread::sleep_for(milliseconds(200));
                if (must_redraw_) _show();
            }
        }).detach();
    }


    const size_t total_;
    std::atomic<size_t> done_;
    std::atomic<int> n_tasks_;

    int last_msg_len_;
    bool show_bar_;
    bool must_redraw_;
    std::mutex output_mtx_;
};


}  // namespace utils

#endif  // INCLUDE_PROGRESS_HPP_
