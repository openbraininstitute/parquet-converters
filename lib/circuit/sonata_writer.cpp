/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#include "sonata_writer.h"

#include <functional>
#include <thread>
#include <iostream>
#include <unordered_set>

#include <range/v3/view/join.hpp>

#include "lib/version.h"

//#define NEURON_LOGGING true

namespace neuron_parquet {
namespace circuit {

using namespace arrow;
using namespace ranges;
using namespace std;


static const unordered_set<string> COLUMNS_TO_SKIP{"synapse_id"};


SonataWriter::SonataWriter(const string & filepath,
                                     uint64_t n_records,
                                     const string& population_name)
  : sonata_file_(filepath, population_name, n_records),
    total_records_(n_records),
    population_name_(population_name),
    output_file_offset_(0)
{ }

#ifdef NEURONPARQUET_USE_MPI
SonataWriter::SonataWriter(const string & filepath,
                                     uint64_t n_records,
                                     const MPI_Params& mpi_params,
                                     uint64_t output_offset,
                                     const string& population_name)
  : sonata_file_(filepath, population_name, mpi_params.comm, mpi_params.info, n_records),
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


void SonataWriter::setup(const CircuitData::Schema* schema, std::shared_ptr<const CircuitData::Metadata> metadata) {
    for (int i = 0; i < schema->num_columns(); ++i) {
        const auto& col = schema->Column(i);
        const auto col_name = col->name();
        if (COLUMNS_TO_SKIP.count(col_name) > 0) {
            continue;
        }

        const auto col_type = parquet_types_to_h5(col->physical_type(), col->converted_type());
        if (col_type < 0) {
            throw std::runtime_error("column " + col_name + " cannot be converted");
        }
        if (!sonata_file_.has_dataset(col_name)) {
            sonata_file_.create_dataset(col_name, col_type, 0, 1);
        }
    }

    std::unordered_map<std::string, std::string> kv;
    metadata->ToUnorderedMap(&kv);
    for (const auto& p: kv) {
        // rfind is poor man's `starts_with`
        if (p.first == "ARROW:schema" || p.first.rfind("org.apache.spark", 0) != std::string::npos) {
            continue;
        } else if (p.first == "source_population_name") {
            sonata_file_.create_dataset_attribute("source_node_id", "node_population", p.second);
        } else if (p.first == "target_population_name") {
            sonata_file_.create_dataset_attribute("target_node_id", "node_population", p.second);
        } else if (p.first == "source_population_size") {
            source_size_ = std::stoul(p.second);
        } else if (p.first == "target_population_size") {
            target_size_ = std::stoul(p.second);
        } else {
            sonata_file_.create_attribute(p.first, p.second);
        }
    }
    sonata_file_.create_attribute("parquet2hdf5_version", neuron_parquet::VERSION);
}


// ================================================================================================
///
/// \brief SonataWriter::write Handles incoming data and
///        distrubutes columns by respective file writer queues
///
void SonataWriter::write(const CircuitData * data, uint length) {
    if( !data || !data->row_group || length == 0 ) {
        return;
    }

    shared_ptr<Table> row_group(data->row_group);
    int n_cols = row_group->num_columns();
    auto names = row_group->ColumnNames();

    for(int i=0; i<n_cols; i++) {
        auto col = row_group->column(i);
        if (COLUMNS_TO_SKIP.count(names[i]) > 0) {
            continue;
        }
        write_data(sonata_file_[names[i]], output_file_offset_, col);
    }

    output_file_offset_ += row_group->num_rows();

}


inline hid_t parquet_types_to_h5(parquet::Type::type t, parquet::ConvertedType::type l) {
    switch (l) {
        case parquet::ConvertedType::UINT_8:
            return H5T_STD_U8LE;
        case parquet::ConvertedType::UINT_16:
            return H5T_STD_U16LE;
        case parquet::ConvertedType::UINT_32:
            return H5T_STD_U32LE;
        case parquet::ConvertedType::UINT_64:
            return H5T_STD_U64LE;
        case parquet::ConvertedType::INT_8:
            return H5T_STD_I8LE;
        case parquet::ConvertedType::INT_16:
            return H5T_STD_I16LE;
        case parquet::ConvertedType::INT_32:
            return H5T_STD_I32LE;
        case parquet::ConvertedType::INT_64:
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

void SonataWriter::write_data(SonataFile::Dataset& dataset,
                              uint64_t offset,
                              const shared_ptr<const ChunkedArray>& col_data) {

    #ifdef NEURON_LOGGING
    cerr << "Writing data... " <<  col_data->length() << " records." << endl;
    #endif

    if (col_data->type()->id() != Type::STRUCT) {
        // get chunks and retrieve the raw data from the buffer
        for (const shared_ptr<Array> & chunk : col_data->chunks()) {
            auto buffer = static_cast<PrimitiveArray*>(chunk.get())->values();
            dataset.write(buffer->data(), chunk->length(), offset);
            offset += chunk->length();
        }
    } else {
        std::cerr << "ERROR: unsupported column type" << std::endl;
        throw std::runtime_error("Unsupported dataset");
    }
}


}} //NS
