#include "circuit_writer_syn2.h"
#include <functional>
#include <thread>
#include <iostream>

//#define NEURON_LOGGING true

namespace neuron_parquet {
namespace circuit {

using namespace arrow;
using namespace std;


CircuitWriterSYN2::CircuitWriterSYN2(const string & destination_dir, uint64_t n_records)
    : destination_dir_(destination_dir),
      total_records_(n_records),
      output_part_length_(n_records),
      output_file_offset_(0),
      output_part_id_(0)
{ }


// ================================================================================================
///
/// \brief CircuitWriterSYN2::write Handles incoming data and
///        distrubutes columns by respective file writer queues
///
void CircuitWriterSYN2::write(const CircuitData * data, uint length) {
    if( !data || !data->row_group || length == 0 ) {
        return;
    }

    #ifdef NEURON_DEBUG
    cout << "Writing data block with " << data->row_group->num_rows() << " rows." << endl;
    #endif

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
        //for( int col_i : cols_to_process) {
            int col_i = 0;
            auto col = row_group->column(col_i);
            const auto & col_name = col->name();
            Type::type t_id(col->type()->id());
            hid_t t = parquet_types_to_h5(t_id);

            if(t == -1) {
                cerr << "Type not supported. Skipping column " << col_name << endl;
                continue;
            }

            ZeroMemQ_Column & q = get_create_handler_for_column(col);

            if( ! q.try_push(col)) {
                remaining_cols.push_back(col_i);
            }
        //}

        if( remaining_cols.size() > 0 ) {
            // Avoid sleep if no need
            this_thread::sleep_for(chrono::milliseconds(10));
        }

        // Move also empties original
        cols_to_process = move(remaining_cols);

    }

}


// ================================================================================================
///
/// \brief CircuitWriterSYN2::create_handles_existing_files
/// \param data The arrow::Table object
/// \param hids The hdfs file hid
/// \param offset The offset where to append to the file
/// \param part_id An identifier of the data part for logging purposes
///
void CircuitWriterSYN2::set_output_block_position(int part_id, uint64_t offset, uint64_t part_length) {
    output_part_id_ = part_id;
    output_file_offset_ = offset;
    output_part_length_ = part_length;
}


// ================================================================================================
///
/// \brief CircuitWriterSYN2::create_handler_for_column Initializes output file and writing thread for a single column
///
ZeroMemQ_Column & CircuitWriterSYN2::get_create_handler_for_column(const shared_ptr<Column> col) {
    int idx;
    const string col_name(col->name());

    if( col_name_to_idx_.count(col_name) > 0) {
        idx = col_name_to_idx_[col_name];
        return *column_writer_queues_[idx];
    }

    #ifdef NEURON_LOGGING
        cerr << "Creating new column and thread for " << col_name << endl;
    #endif
    idx = column_writer_queues_.size();
    col_name_to_idx_[col_name] = idx;

    h5_ids h5_file = init_h5file(col_name + ".h5", col);
    std::unique_ptr<ZeroMemQ_Column> queue(new ZeroMemQ_Column(idx));

    column_writer_queues_.push_back(std::move(queue)); // move the unique_ptr
    ZeroMemQ_Column & q = *column_writer_queues_.back();
    create_thread_process_data(h5_file, q);

    return q;
}


// ================================================================================================
///
/// \brief CircuitWriterSYN2::init_h5file
///
h5_ids CircuitWriterSYN2::init_h5file(const string & filepath, shared_ptr<arrow::Column> column) {
    string destination = destination_dir_ + "/" + filepath;
    hsize_t dims[1] = { total_records_ };


    hid_t file_id;

    if(use_mpio_) {
        hid_t plist_id = H5Pcreate(H5P_FILE_ACCESS);
        H5Pset_fapl_mpio(plist_id, mpi_.comm, mpi_.info);
        file_id = H5Fcreate(destination.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, plist_id);
        H5Pclose(plist_id);
    }
    else {
        file_id = H5Fcreate(destination.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    }

    // Dataspace create
    hid_t dataspace_id = H5Screate_simple(1, dims, NULL);

    Type::type t (column->type()->id());
    string col_name (string("/") + column->name());

    // Dataset
    hid_t ds_id = H5Dcreate2(file_id, col_name.c_str(), parquet_types_to_h5(t), dataspace_id,
                       H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    // Once ds is created we change the filespace relative to ds
    H5Sclose(dataspace_id);

    h5_ids output_id {file_id, ds_id, H5Dget_space(ds_id), H5P_DEFAULT};

    if(use_mpio_) {
        output_id.plist = H5Pcreate(H5P_DATASET_XFER);
    }

    files_.push_back(output_id);
    return output_id;
}


// ================================================================================================
///
/// \brief CircuitWriterSYN2::use_mpio
///
void CircuitWriterSYN2::use_mpio(MPI_Comm comm, MPI_Info info) {
    use_mpio_ = true;
    mpi_.comm = comm;
    mpi_.info = info;
}


// ================================================================================================
///
/// \brief CircuitWriterSYN2::close_files
///
void CircuitWriterSYN2::close_files() {
    shared_ptr<Column> _null;
    for(auto & q : column_writer_queues_) {
        q->blocking_push(_null);
    }

    // Wait for threads to finish
    for (thread& t : threads_) {
        if(t.joinable()) {
            t.join();
        }
    }
    for (h5_ids& outstream : files_) {
        H5Dclose(outstream.ds);
        H5Sclose(outstream.dspace);
        if(outstream.plist != H5P_DEFAULT) {
            H5Pclose(outstream.plist);
        }
        H5Fclose(outstream.file);
    }
}



// ================================================================================================
/// --------------------------
/// The THREAD to process data
/// --------------------------

void CircuitWriterSYN2::create_thread_process_data(const h5_ids h5_output,
                                                   ZeroMemQ_Column & q) {
    static int writer_count = 0;
    int writer_id = writer_count ++;
    #ifdef NEURON_DEBUG
    cerr << "Initting thread on queue " << q.id() << endl;
    #endif

    auto f = [this, h5_output, &q, writer_id]{
        uint64_t cur_offset = this->output_file_offset_;

        while(true) {
            #ifdef NEURON_DEBUG
                cerr << "Thread " << writer_id << " Gonna read data from queue " << q.id() << endl;
            #endif

            shared_ptr<Column> column_ptr(q.blocking_pop());
            if( !column_ptr ) {
                break;
            }
            write_data(h5_output, cur_offset, column_ptr);

        }

        #ifdef NEURON_DEBUG
        cout << "\rTerminating writer " << writer_id << "\n" << endl;
        #endif
    };

    thread t(f);
    threads_.push_back(std::move(t));
}


// ================================================================================================

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


// ================================================================================================

void write_data(const h5_ids h5_output, uint64_t& offset,
                const shared_ptr<const Column>& col_data) {

    static thread_local Type::type t_id(col_data->type()->id());
    static thread_local hid_t t = parquet_types_to_h5(t_id);

    #ifdef NEURON_LOGGING
    cerr << "Writing data... " <<  col_data->length() << " records." << endl;
    #endif

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

            hsize_t memspace = H5Screate_simple(1, &buf_len, NULL);
            H5Sselect_hyperslab(h5_output.dspace, H5S_SELECT_SET, &offset_, NULL, &buf_len, NULL);

            H5Dwrite(h5_output.ds, t, memspace, h5_output.dspace, h5_output.plist, buf->data());
            H5Sclose(memspace);

            offset += buf_len;
        }
    }
}


}} //NS
