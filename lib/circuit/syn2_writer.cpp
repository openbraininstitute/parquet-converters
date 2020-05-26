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
#include <unordered_set>

#include <range/v3/view/join.hpp>

//#define NEURON_LOGGING true

namespace neuron_parquet {
namespace circuit {

using namespace arrow;
using namespace ranges;
using namespace std;


const string CircuitWriterSYN2::DEFAULT_POPULATION_NAME(DEFAULT_SYN2_POPULATION_NAME);


const std::vector<std::string> CircuitWriterSYN2::nested_cols{"x", "y", "z"};


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


void throw_invalid_column(const std::string& col_name,
                          const std::unordered_set<std::string>& names,
                          const std::vector<std::string>& notfound) {
    std::string msg = "wrong nesting for column " + col_name;
    msg += ": ";
    if (names.size() > 0) {
        msg += "unrecognized subcolumn(s) ";
        msg += view::join(names, ", ");
    }
    if (names.size() > 0 and notfound.size() > 0)
        msg += " and ";
    if (notfound.size() > 0) {
        msg += "missing subcolumn(s) ";
        msg += view::join(notfound, ", ");
    }
    throw std::runtime_error(msg);
}


void CircuitWriterSYN2::setup(const CircuitData::Schema* schema) {
    for (int i = 0; i < schema->num_columns(); ++i) {
        const auto& col = schema->Column(i);
        const auto col_name = col->name();
        const auto col_type = parquet_types_to_h5(col->physical_type(), col->logical_type());
        if (col_type < 0) {
            throw std::runtime_error("column " + col_name + " cannot be converted");
        }
        if (!syn2_file_.has_dataset(col_name)) {
            syn2_file_.create_dataset(col_name, col_type, 0, 1);
        }
    }
}


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
        auto col_name = col->name();
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


inline hid_t parquet_types_to_h5(parquet::Type::type t, parquet::LogicalType::type l) {
    switch (l) {
        case parquet::LogicalType::UINT_8:
            return H5T_STD_U8LE;
        case parquet::LogicalType::UINT_16:
            return H5T_STD_U16LE;
        case parquet::LogicalType::UINT_32:
            return H5T_STD_U32LE;
        case parquet::LogicalType::UINT_64:
            return H5T_STD_U64LE;
        case parquet::LogicalType::INT_8:
            return H5T_STD_I8LE;
        case parquet::LogicalType::INT_16:
            return H5T_STD_I16LE;
        case parquet::LogicalType::INT_32:
            return H5T_STD_I32LE;
        case parquet::LogicalType::INT_64:
            return H5T_STD_I64LE;
        default:
            break;
    }
    switch (t) {
        case parquet::Type::INT32:
            return H5T_STD_I32LE;
        case parquet::Type::INT64:
            return H5T_STD_I64LE;
        case parquet::Type::FLOAT:
            return H5T_IEEE_F32LE;
        case parquet::Type::DOUBLE:
            return H5T_IEEE_F64LE;
        default:
            break;
    }
    std::cerr << "attempt to convert an unknown datatype!" << std::endl;
    return -1;
}


// ================================================================================================

void CircuitWriterSYN2::write_data(Syn2CircuitHdf5::Dataset& dataset,
                                   uint64_t offset,
                                   const shared_ptr<const Column>& col_data) {

    #ifdef NEURON_LOGGING
    cerr << "Writing data... " <<  col_data->length() << " records." << endl;
    #endif

    if (col_data->type()->id() != Type::STRUCT) {
        // get chunks and retrieve the raw data from the buffer
        for (const shared_ptr<Array> & chunk : col_data->data()->chunks()) {
            auto buffer = static_cast<PrimitiveArray*>(chunk.get())->values();
            dataset.write(buffer->data(), chunk->length(), offset);
            offset += chunk->length();
        }
    } else {
        for (const shared_ptr<Array> & chunk : col_data->data()->chunks()) {
            auto array = dynamic_cast<StructArray*>(chunk.get());

            for (std::size_t i = 0; i < nested_cols.size(); ++i) {
                const auto& n = nested_cols[i];
                auto buffer = dynamic_cast<PrimitiveArray*>(array->GetFieldByName(n).get())->values();
                dataset.write(buffer->data(), i, chunk->length(), offset);
            }
        }
    }
}


}} //NS
