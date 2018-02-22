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
        #ifdef NEURON_DEBUG
            std::cerr << "Filling queue " << this_id << std::endl;
        #endif
        // shared_ptr that stays is copied
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
        // queue shared_ptr no longer needed, can be pilfered
        col = std::move(column_to_process);
        has_data = false;
        return true;
    }

    std::shared_ptr<T> blocking_pop() {
        std::shared_ptr<T> col;
        while(!try_pop(col)) {
            #ifdef NEURON_DEBUG
                std::cerr << "Thread " << this_id << " waiting for data..." << std::endl;
            #endif
            std::this_thread::sleep_for(
                std::chrono::milliseconds(10));
        }
        #ifdef NEURON_DEBUG
            std::cerr << "Thread " << this_id << " POP!" << std::endl;
        #endif
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
    // designated has_data var since a null shared_ptr must be able to go throught the queue
    bool has_data;
    std::shared_ptr<T> column_to_process;
    std::mutex m_;

};




#endif // ZEROMEMQ_COLUMN_H
