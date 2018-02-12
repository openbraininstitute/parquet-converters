#include "circuit_writer_syn2.h"
#include <functional>

namespace neuron_parquet {
namespace circuit {

using namespace arrow;


CircuitWriterSYN2::CircuitWriterSYN2()
{ }



void CircuitWriterSYN2::write(CircuitData* data, uint length){
//

    std::shared_ptr<Table> row_group = std::move(data->row_group);
    int n_cols = data->row_group->num_columns;

    // First execution
    if( data_columns_to_process.size() == 0 )  {
        data_columns_to_process.resize(n_cols);

        for(int i=0; n_cols; i++) {
            std::shared_ptr<Column> col = row_group->column(i);
            h5_file = init_h5file(filename, col);
            // data_columns_to_process is always kept internally by reference
            writer_threads.push_back( create_thread_process_data(h5_file, data_columns_to_process[i]) );
        }
    }

    // Initially all threads are considered
    std::vector<int> running_threads;
    for (i=0; i<n_cols; i++) {
        running_threads.push_back(i);
    }

    while( running_threads.size() > 0 ) {
        std::vector<int> still_running_threads;

        for(int i : running_threads) {
            // Check we can write data
            if( data_columns_to_process[i] != nullptr ) {
                still_running_threads.push_back(i);
            }
            else {
                std::shared_ptr<Column> col = row_group->column(i);
                data_columns_to_process[i] = col;
                // Fetch thread and notify
                writer_threads[i].notify();
            }
        }

        running_threads = std::move(still_running_threads);

        if( running_threads.size()>0 ) {
            // Avoid sleep if no need
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

}



template <typename T>
inline void dump_data(h5_file_handler, u_int8_t* buffer) {

}





inline void write_data(h5_file_handler,
                       const std::shared_ptr<const Column>& col_data) {
    // get data in buffer
    for( std::shared_ptr<Array> & chunk : col_data->data->chunks() ) {
        for( std::shared_ptr<Buffer> & buf : chunk->data()->buffers() ) {

        }
    }
}


void init_h5file(const std::string & filename, const std::shared_ptr<const Column>& column) {

}


std::shared_ptr<std::thread> CircuitWriterSYN2::create_thread_process_data(h5 h5_file,
                                                                           std::shared_ptr<const Column>& column) {
    auto f = [&h5_file](){
        // Need primitives for blocking/notify

        write_data(h5_file, col_data);
        // set buffer to null (available to receive more data)
        col_data = nullptr;
    };

    std::shared_ptr<std::thread> thread = new std::thread(f);
    return thread;
}




}} //NS
