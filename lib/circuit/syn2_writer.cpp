/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#include "syn2_writer.h"
#include <functional>
#include <thread>
#include <iostream>

//#define NEURON_LOGGING true

namespace neuron_parquet {
namespace circuit {

using namespace arrow;
using namespace std;


const string CircuitWriterSYN2::DEFAULT_POPULATION_NAME(DEFAULT_SYN2_POPULATION_NAME);


CircuitWriterSYN2::CircuitWriterSYN2(const string & filepath,
                                     uint64_t n_records,
                                     const string& population_name)
  : syn2_file_(filepath, population_name, n_records),
    total_records_(n_records),
    population_name_(population_name),
    output_file_offset_(0)
{ }

#ifdef NEURONPARQUET_USE_MPI
CircuitWriterSYN2::CircuitWriterSYN2(const string & filepath,
                                     uint64_t n_records,
                                     const MPI_Params& mpi_params,
                                     uint64_t output_offset,
                                     const string& population_name)
  : syn2_file_(filepath, population_name, mpi_params.comm, mpi_params.info, n_records),
    total_records_(n_records),
    population_name_(population_name),
    output_file_offset_(output_offset)
{ }
#endif


// ================================================================================================
///
/// \brief CircuitWriterSYN2::write Handles incoming data and
///        distrubutes columns by respective file writer queues
///
void CircuitWriterSYN2::write(const CircuitData * data, uint length) {
    if( !data || !data->row_group || length == 0 ) {
        return;
    }

    shared_ptr<Table> row_group(data->row_group);
    int n_cols = row_group->num_columns();

    for(int i=0; i<n_cols; i++) {
        shared_ptr<Column> col(row_group->column(i));
        const string& col_name = col->name();

        if( !syn2_file_.has_dataset(col_name) ){
            Type::type t (col->type()->id());
            syn2_file_.create_dataset(col_name, parquet_types_to_h5(t));
        }

        write_data(syn2_file_[col_name], output_file_offset_, col);
    }

    output_file_offset_ += row_group->num_rows();

}


const std::vector<string> CircuitWriterSYN2::dataset_names() {
    vector<string> keys(syn2_file_.datasets().size());
    for( const auto& item : syn2_file_.datasets() ) {
        keys.push_back(item.first);
    }
    return keys;
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
        case Type::INT64:
            return H5T_STD_I64LE;
        case Type::UINT8:
            return H5T_STD_U8LE;
        case Type::UINT16:
            return H5T_STD_U16LE;
        case Type::UINT32:
            return H5T_STD_U32LE;
        case Type::UINT64:
            return H5T_STD_U64LE;
        case Type::FLOAT:
            return H5T_IEEE_F32LE;
        case Type::DOUBLE:
            return H5T_IEEE_F64LE;
        case Type::STRING:
            return H5T_C_S1;
        default:
            std::cerr << "attempt to convert an unknown datatype!" << std::endl;
            return -1;
    }
}


// ================================================================================================

void write_data(Syn2CircuitHdf5::Dataset& dataset,
                uint64_t offset,
                const shared_ptr<const Column>& col_data) {

    #ifdef NEURON_LOGGING
    cerr << "Writing data... " <<  col_data->length() << " records." << endl;
    #endif

    // get chunks and retrieve the raw data from the buffer
    for( const shared_ptr<Array> & chunk : col_data->data()->chunks() ) {
        uint64_t buf_len = chunk->length();

        // 1 is the position containing values for PrimitiveArrays (otherwise we need a static pointer cast)
        shared_ptr<Buffer> buffer = chunk->data()->buffers[1];

        dataset.write(buffer->data(), buf_len, offset);
        offset += buf_len;
    }
}


}} //NS
