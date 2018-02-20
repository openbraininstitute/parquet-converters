#ifndef ZEROMEMQ_H
#define ZEROMEMQ_H

#include <thread>
#include <mutex>
#include <iostream>

template <typename T>
class ZeroMemQ{
public:
    ZeroMemQ<T>(int id_=0)
    : this_id(id_),
      has_data(false)
    { }

    // No copies shall be made
    ZeroMemQ<T>(const ZeroMemQ<T> & other) = delete;

    ZeroMemQ<T>(ZeroMemQ<T> && other)
    : this_id(other.this_id)
    {
        std::lock_guard<std::mutex> lock2(other.m_);
        has_data = other.has_data;
        column_to_process = std::move(other.column_to_process);
    }

    ZeroMemQ<T>& operator=(const ZeroMemQ<T> & other) = delete;

    ZeroMemQ<T>& operator=(ZeroMemQ<T> && other){
        std::lock_guard<std::mutex> lock(m_);
        std::lock_guard<std::mutex> lock2(other.m_);
        this_id = other.this_id;
        has_data = other.has_data;
        column_to_process = std::move(other.column_to_process);
    }


    inline bool try_push(const std::shared_ptr<T> &col) {
        std::lock_guard<std::mutex> lock(m_);
        if( has_data ) { return false; }
        std::cerr << "Filling queue " << this_id << std::endl;
        column_to_process = col;
        has_data = true;
        return true;
    }


    void blocking_push(const std::shared_ptr<T> &col) {
        while(! try_push(col)) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(10));
        }
    }


    inline bool try_pop(std::shared_ptr<T> &col) {
        std::lock_guard<std::mutex> lock(m_);
        if( ! has_data ) { return false; }
        // queue shared_ptr can be pilfered
        col = std::move(column_to_process);
        has_data = false;
        return true;
    }

    std::shared_ptr<T> blocking_pop() {
        std::shared_ptr<T> col;
        while(!try_pop(col)) {
            std::cerr << "thread " << this_id << " waiting for data..." << std::endl;
            std::this_thread::sleep_for(
                std::chrono::milliseconds(10));
        }
        std::cerr << "thread " << this_id << " POP!" << this_id << std::endl;
        return col;
    }


    bool is_available() const {
        return !has_data;
    }

    int id() const{
        return this_id;
    }

private:
    int this_id;
    bool has_data;
    std::shared_ptr<T> column_to_process;
    std::mutex m_;

};




#endif // ZEROMEMQ_COLUMN_H
