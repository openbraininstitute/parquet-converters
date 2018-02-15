#include "circuit_writer_syn2.h"
#include <functional>
#include <thread>

namespace neuron_parquet {
namespace circuit {

using namespace arrow;



CircuitWriterSYN2::CircuitWriterSYN2(const std::string & destination_dir)
    : destination_dir_(destination_dir)
{ }


///
/// \brief CircuitWriterSYN2::write Handles incoming data and
///        distrubutes columns by respective file writer queues
/// \param data
/// \param length
///
void CircuitWriterSYN2::write(const CircuitData * data, uint length) {
    std::shared_ptr<Table> row_group(data->row_group);
    int n_cols = row_group->num_columns();

    std::vector<int> cols_to_process(n_cols);
    std::vector<int> remaining_cols;

    for(int i=0; n_cols; i++) {
        cols_to_process.push_back(i);
    }


    // The rationale is to loop through all the columns at a time, creating new
    //   threads if necessary. If some threads are not ready then immediately skip.
    // At the end, if there are columns that could not be dispatched sleep 10 ms and
    //   repeat the cycle

    while( cols_to_process.size() >0 ) {
        for( int col_i : cols_to_process) {
            auto col = row_group->column(col_i);
            const auto & col_name = col->name();
            int idx = -1;
            if( col_name_to_idx_.count(col_name) > 0) {
                idx = col_name_to_idx_[col_name];
            }

            // Create new column
            if( idx == -1) {
                printf("Creating new column and thread for %s\n", col_name.c_str());
                idx = column_writer_queues_.size();
                col_name_to_idx_[col_name] = idx;

                column_writer_queues_.push_back(ZeroMemQ_Column());

                h5 h5_file = init_h5file(std::string("syn2prop") + col_name, col);
                create_thread_process_data(h5_file, column_writer_queues_.back());
            }

            ZeroMemQ_Column & q = column_writer_queues_[idx];

            if(! q.try_push(col)) {
                //Thread not ready yet
                remaining_cols.push_back(col_i);
            }
        }

        if( remaining_cols.size()>0 ) {
            // Avoid sleep if no need
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        // Move also empties original
        cols_to_process = std::move(remaining_cols);

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


void CircuitWriterSYN2::create_thread_process_data(h5 h5_file,
                                                   ZeroMemQ_Column & queue) {
    auto f = [&h5_file, &queue]{
        std::shared_ptr<Column> column_ptr = queue.blocking_pop();

        while(column_ptr != nullptr) {
            write_data(h5_file, column_ptr);
            column_ptr = queue.blocking_pop();
        }
    };

    std::thread t(f);
    t.detach();
}






}} //NS
