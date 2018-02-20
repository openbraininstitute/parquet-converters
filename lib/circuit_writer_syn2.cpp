#include "circuit_writer_syn2.h"
#include <functional>
#include <thread>
#include <hdf5.h>
#include <iostream>


namespace neuron_parquet {
namespace circuit {

using namespace arrow;
using namespace std;


CircuitWriterSYN2::CircuitWriterSYN2(const string & destination_dir, uint64_t n_records)
    : destination_dir_(destination_dir),
      total_records_(n_records)
{ }


///
/// \brief CircuitWriterSYN2::write Handles incoming data and
///        distrubutes columns by respective file writer queues
/// \param data
/// \param length
///
void CircuitWriterSYN2::write(const CircuitData * data, uint length) {
    if( data == nullptr || length == 0 ) {
        shared_ptr<Column> _null;
        for(auto & q : column_writer_queues_) {
            q->blocking_push(_null);
        }
        return;
    }

    shared_ptr<Table> row_group(data->row_group);
    int n_cols = row_group->num_columns();

    vector<int> cols_to_process(n_cols);
    vector<int> remaining_cols;

    for(int i=0; i<n_cols; i++) {
        cols_to_process[i]=i;
    }


    // The rationale is to loop through all the columns at a time, creating new
    //   threads if necessary. If some threads are not ready then immediately skip.
    // At the end, if there are columns that could not be dispatched sleep 10 ms and
    //   repeat the cycle

    while( cols_to_process.size() >0 ) {
        for( int col_i : cols_to_process) {
            auto col = row_group->column(col_i);
            const auto & col_name = col->name();
            Type::type t_id(col->type()->id());
            hid_t t = parquet_types_to_h5(t_id);
            if(t == -1) {

                cerr << "Type not supported. Skipping column " << col_name << endl;
                continue;
            }

            int idx = -1;
            if( col_name_to_idx_.count(col_name) > 0) {
                idx = col_name_to_idx_[col_name];
            }

            // Create new column
            if( idx == -1) {
                cerr << "Creating new column and thread for " << col_name << endl;
                idx = column_writer_queues_.size();
                col_name_to_idx_[col_name] = idx;

                h5_ids h5_file = init_h5file(col->name(), col);
                std::unique_ptr<ZeroMemQ_Column> queue(new ZeroMemQ_Column(idx));
                create_thread_process_data(h5_file, *queue);

                column_writer_queues_.push_back(std::move(queue));
            }

            std::unique_ptr<ZeroMemQ_Column> & q = column_writer_queues_[idx];

            cerr << "Feeding data to queue " << idx << endl;
            if( ! q->try_push(col)) {
                //Thread not ready yet
                cerr << "Postponded since queue " << idx << "full" << col_name << endl;
                remaining_cols.push_back(col_i);
            }
        }

        if( remaining_cols.size()>0 ) {
            // Avoid sleep if no need
            this_thread::sleep_for(chrono::milliseconds(10));
        }

        // Move also empties original
        cols_to_process = move(remaining_cols);

    }

}


h5_ids CircuitWriterSYN2::init_h5file(const string & filepath, shared_ptr<arrow::Column> column) {
    string destination = destination_dir_ + "/" + filepath;
    hid_t file_id = H5Fcreate(destination.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    hsize_t dims[1] = {total_records_};

    hid_t dataspace_id = H5Screate_simple(1, dims, NULL);
    Type::type t(column->type()->id());
    string col_name("/");
    col_name += column->name();

    hid_t ds_id = H5Dcreate2(file_id, col_name.c_str(), parquet_types_to_h5(t), dataspace_id,
                      H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    return {file_id, ds_id, dataspace_id};
}


void CircuitWriterSYN2::close_files() {
    write(nullptr, 0);

    // Wait for threads to finish
    for (thread& t : threads_) {
        t.join();
    }
}



void CircuitWriterSYN2::create_thread_process_data(const h5_ids h5_output,
                                                   ZeroMemQ_Column & q) {
    static int writer_count = 0;
    int writer_id = writer_count ++;
    cerr << "Initting thread on queue " << q.id() << endl;

    auto f = [h5_output, &q, writer_id]{
        uint64_t cur_offset = 0;

        while(true) {
            cerr << "Thread " << writer_id << " Gonna read data from queue " << q.id() << endl;
            shared_ptr<Column> column_ptr(q.blocking_pop());
            if( !column_ptr ) {
                break;
            }
            write_data(h5_output, cur_offset, column_ptr);
        }

        cerr << "Closing file from writer " << writer_id << endl;
        H5Dclose(h5_output.ds);
        H5Fclose(h5_output.file);
    };

    thread t(f);
    threads_.push_back(std::move(t));
}



inline hid_t parquet_types_to_h5(Type::type t) {
    switch( t ) {
        case Type::INT8:
            return H5T_STD_I8LE;
        case Type::INT16:
            return H5T_STD_I16LE;
        case Type::INT32:
            return H5T_STD_I32LE;
        case Type::UINT8:
            return H5T_STD_U8LE;
        case Type::UINT16:
            return H5T_STD_U16LE;
        case Type::UINT32:
            return H5T_STD_U32LE;
        case Type::FLOAT:
            return H5T_IEEE_F32LE;
        case Type::DOUBLE:
            return H5T_IEEE_F64LE;
        case Type::STRING:
            return H5T_C_S1;
        default:
            return -1;
    }
}


void write_data(const h5_ids h5_output, uint64_t& offset,
                const shared_ptr<const Column>& col_data) {

    static thread_local Type::type t_id(col_data->type()->id());
    static thread_local hid_t t = parquet_types_to_h5(t_id);

    cerr << "Writing data... " <<  col_data->length() << " records." << endl;

    // get data in buffers
    for( const shared_ptr<Array> & chunk : col_data->data()->chunks() ) {
        for( const shared_ptr<Buffer> & buf : chunk->data()->buffers ) {
            if( !buf ) {
                // Some buffers might be empty
                continue;
            }
            // convert and set const
            const hsize_t offset_ = offset;
            const hsize_t buf_len = chunk->length();

            hid_t mem_space = H5Screate_simple(1, &buf_len, nullptr);
            H5Sselect_hyperslab(h5_output.dspace, H5S_SELECT_SET, &offset_, NULL, &buf_len, NULL);

            H5Dwrite(h5_output.ds, t, mem_space, h5_output.dspace, H5P_DEFAULT, buf->data());
            H5Sclose(mem_space);
            offset += buf_len;
        }
    }
}




}} //NS
