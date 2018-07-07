#ifndef PROGRESS_HPP
#define PROGRESS_HPP

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <stdexcept>
#include <chrono>
#include <mutex>
#include <atomic>

using namespace std::chrono;

class ProgressMonitor {
public:
    ProgressMonitor(size_t total, bool show_bar)
        : total_(total)
        , done_(0)
        , n_tasks_(1)
        , last_msg_len(0)
        , show_bar_(show_bar)
        , t0(system_clock::now())
    {
        if(total<=0)
            throw std::runtime_error("Total blocks cant be zero");
        _show();
    }

    ProgressMonitor(size_t total=100)
        : ProgressMonitor(total, isatty(STDERR_FILENO))
    { }

    ~ProgressMonitor() {
        if( show_bar_ )        
            clear();
    }


    inline ProgressMonitor& operator +=(int blocks_done) {
        update_mtx_.lock();
        done_+= blocks_done;
        update_mtx_.unlock();
        _show();
        return *this;
    }
    
    inline void next() {
        (*this) += 1;
    }

    void set_parallelism(int n_tasks) {
        n_tasks_ = n_tasks;
    }

    void add_task() {
        n_tasks_++;
        _show();
    }

    void del_task() {
        n_tasks_--;
        _show();
    }


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
        SubTask(ProgressMonitor& pm) : pm_(pm) {
            pm.add_task();
        }

     private:
        ProgressMonitor& pm_;
    };

    SubTask subtask() {
        return SubTask(*this);
    }


 protected:
    static const int MAX_BAR_LEN = 100;

    inline void _show(){
        static const char* PB_STR = "=================================================="
                                    "==================================================";
        if (!show_bar_ || !progress_mtx_.try_lock()) {
            return;
        }
        time_point<system_clock> t = system_clock::now();
        auto fp_ms = t - t0;
        if( fp_ms < milliseconds(100)  && done_ < total_ && done_ > 0) {
            // Will show first, last and at most once every 0.1 sec
            return;
        }

        t0 = t;
        float progress = (float)done_ / total_;
        int cols = window_cols()-1;

        char tasks_str[30];
        sprintf(tasks_str, "(%ld + %d / %ld)", done_, n_tasks_, total_);

        int progress_len = cols - ( 11 + strlen(tasks_str) );
        if (progress_len > MAX_BAR_LEN) {
            progress_len = MAX_BAR_LEN;
        }

        int bar_len = (int) (progress * progress_len);
        int rpad = progress_len - bar_len;
        last_msg_len = progress_len + 11 + strlen(tasks_str);

        fprintf(stderr, "\r[%5.1f%%|%.*s>%*s] %s",
                progress*100, bar_len, PB_STR, rpad, "", tasks_str);
        progress_mtx_.unlock();
    }

    void clear()  {
        fprintf(stderr, "\r%*s\r", last_msg_len, "");
    }


private:
    static inline int window_cols() {
        struct winsize window;
        int columns=80;

        if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &window) >= 0) {
            if (window.ws_col > 0)
                columns = window.ws_col;
        }
        return columns;

    }

    const size_t total_;
    size_t done_;
    int n_tasks_;
    int last_msg_len;
    const bool show_bar_;
    time_point<system_clock> t0;
    std::mutex update_mtx_, progress_mtx_;

};


#endif // PROGRESS_HPP
