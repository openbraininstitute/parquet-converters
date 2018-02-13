#include "circuit_writer_syn2.h"
#include <functional>

namespace neuron_parquet {
namespace circuit {

using namespace arrow;



CircuitWriterSYN2::CircuitWriterSYN2()
{ }



void CircuitWriterSYN2::write(CircuitData* data, uint length){
    std::shared_ptr<Table> row_group = std::move(data->row_group);
    int n_cols = data->row_group->num_columns();

    std::vector<int> cols_to_process(n_cols);
    std::vector<int> remaining_cols(n_cols);
    std::vector<int> & r_processing_cols = cols_to_process;

    for(int i=0; n_cols; i++) {
        cols_to_process.push_back(i);
    }

    while( r_processing_cols.size() >0 ) {
        remaining_cols.clear();

        for( int col_i : cols_to_process) {
            std::shared_ptr<Column> col = row_group->column(col_i);
            const std::string& col_name = col->name();
            auto item = col_name_to_idx.find(col_name);
            int idx = -1;

            // Create new column
            if( item == col_name_to_idx.end()) {
                idx = writer_threads.size();
                h5 h5_file = init_h5file(std::string("syn2pop") + col_name, col);
                // data_columns_to_process is always kept internally by reference
                col_name_to_idx[col_name] = idx;
                data_columns_to_process.resize(idx+1);
                // copy ptr
                data_columns_to_process[idx] = col;
                // Create thread with ref to data_col
                writer_threads.push_back( create_thread_process_data(h5_file, data_columns_to_process[idx]) );
            }
            else {
                // Thread exists, we must feed it if it's starving
                if( data_columns_to_process[idx] == nullptr ) {
                    data_columns_to_process[idx] = col;
                }
                else {
                    //Thread not ready yet
                    remaining_cols.push_back(col_i);
                }
            }
        }

        if( remaining_cols.size()>0 ) {
            // Avoid sleep if no need
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        r_processing_cols = remaining_cols;
    }

}



template <typename T>
inline void dump_data(h5 h5_file_handler, u_int8_t* buffer) {

}


inline void write_data(h5 h5_file,
                       const std::shared_ptr<const Column>& col_data) {
    // get data in buffer
    for( const std::shared_ptr<Array> & chunk : col_data->data()->chunks() ) {
        for( const std::shared_ptr<Buffer> & buf : chunk->data()->buffers ) {
            (void) buf;
        }
    }
}


h5 init_h5file(const std::string & filename, std::shared_ptr<Column> column) {
    return 0;
}


const std::shared_ptr<std::thread> CircuitWriterSYN2::create_thread_process_data(h5 h5_file,
                                                                                 std::shared_ptr<Column>& r_column_ptr) {
    auto f = [&h5_file, &r_column_ptr]{

        while(true) {
            // Need primitives for blocking/notify

            write_data(h5_file, r_column_ptr);

            // set buffer to null (available to receive more data)
            r_column_ptr = nullptr;
        }
    };

    const std::shared_ptr<std::thread> thread(new std::thread(f));
    return thread;
}




}} //NS
