#ifndef ZEROMEMQ_H
#define ZEROMEMQ_H

#include <thread>
#include <mutex>

template <typename T>
class ZeroMemQ{
public:
    ZeroMemQ()
    : has_data(false)
    { }

    // No copies shall be made
    ZeroMemQ(const ZeroMemQ<T> & other) = delete;

    ZeroMemQ<T>(ZeroMemQ<T> && other)
        : has_data(other.has_data),
          column_to_process(std::move(other.column_to_process))
        {}


    inline bool try_push(std::shared_ptr<T> &col) {
        std::lock_guard<std::mutex> lock(m_);
        if( has_data ) { return false; }
        column_to_process = col;
        has_data = true;
        return true;
    }


    void blocking_push(std::shared_ptr<T> &col) {
        while(! try_push(col)) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(1));
        }
    }


    std::shared_ptr<T> blocking_pop() {
        while(true) {
            std::lock_guard<std::mutex> lock(m_);
            if( has_data ) {
                has_data = false;
                return column_to_process;
            }
            else{
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    }

    bool is_available() const {
        return !has_data;
    }

private:
    bool has_data;
    std::shared_ptr<T> column_to_process;
    std::mutex m_;
};


#endif // ZEROMEMQ_COLUMN_H
