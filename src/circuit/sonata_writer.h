/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#pragma once

#include <string>
#include <unordered_map>
#include <memory>
#ifdef NEURONPARQUET_USE_MPI
#include <mpi.h>
#endif
#include <hdf5.h>

#include <arrow/api.h>
#include <parquet/types.h>

#include "../generic_writer.h"
#include "circuit_defs.h"
#include "sonata_file.h"


namespace neuron_parquet {
namespace circuit {



///
/// \brief The SonataWriter which writes one hdf5 file per column.
///        Each file is written by a separate thread, so the main thread only
///        dispatches data and is immediately freed to fetch more data.
///
class SonataWriter : public Writer<CircuitData>
{
public:

#ifdef NEURONPARQUET_USE_MPI
    struct MPI_Params {
        MPI_Comm comm;
        MPI_Info info;
    };
#endif

    SonataWriter(const std::string& filepath,
                      uint64_t n_records,
                      const std::string& population_name);

#ifdef NEURONPARQUET_USE_MPI
    SonataWriter(const std::string& filepath,
                      uint64_t n_records,
                      const MPI_Params& mpi_params,
                      uint64_t output_offset,
                      const std::string& population_name);
#endif

    ~SonataWriter() = default;

    virtual void setup(const CircuitData::Schema* schema, std::shared_ptr<const CircuitData::Metadata> metdata) override;

    virtual void write(const CircuitData* data, uint length) override;

    void write_indices(bool parallel = false) {
        sonata_file_.write_indices(source_size_, target_size_, parallel);
    }

private:
    static void write_data(SonataFile::Dataset& ds,
                           uint64_t r_offset,
                           const std::shared_ptr<const arrow::ChunkedArray>& r_col_data);

    SonataFile sonata_file_;

    const uint64_t total_records_;
    const std::string population_name_;
    uint64_t output_file_offset_;

    size_t source_size_ = 0;
    size_t target_size_ = 0;
};


/// \brief Map from Parquet datatypes to HDF5 ones
///
/// Will use the information from `ConvertedType` first to determine
/// customary signed and unsigned integers before falling back to the
/// plain `Type` for generic integer and floating point numbers.
inline hid_t parquet_types_to_h5(parquet::Type::type, parquet::ConvertedType::type);


}}  // namespace neuron_parquet::circuit EOF
