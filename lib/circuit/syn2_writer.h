#ifndef CIRCUITWRITERSYN2_H
#define CIRCUITWRITERSYN2_H

#include "generic_writer.h"
#include "circuit_defs.h"
#include "syn2hdf5.h"
#include <string>
#include <unordered_map>
#include <memory>
#ifdef NEURONPARQUET_USE_MPI
#include <mpi.h>
#endif
#include <hdf5.h>

#define DEFAULT_SYN2_POPULATION_NAME "default"


namespace neuron_parquet {
namespace circuit {



///
/// \brief The CircuitWriterSYN2 which writes one hdf5 file per column.
///        Each file is written by a separate thread, so the main thread only
///        dispatches data and is immediately freed to fetch more data.
///
class CircuitWriterSYN2 : public Writer<CircuitData>
{
public:
    typedef std::string string;

#ifdef NEURONPARQUET_USE_MPI
    struct MPI_Params {
        MPI_Comm comm;
        MPI_Info info;
    };
#endif

    CircuitWriterSYN2(const string& filepath,
                      uint64_t n_records,
                      const string& population_name=DEFAULT_POPULATION_NAME);

#ifdef NEURONPARQUET_USE_MPI
    CircuitWriterSYN2(const string& filepath,
                      uint64_t n_records,
                      const MPI_Params& mpi_params,
                      uint64_t output_offset,
                      const string& population_name=DEFAULT_POPULATION_NAME);
#endif

    ~CircuitWriterSYN2() {}

    virtual void write(const CircuitData* data, uint length) override;

    const std::vector<string> dataset_names();

    Syn2CircuitHdf5& syn2_file () {
        return syn2_file_;
    }

    static const string DEFAULT_POPULATION_NAME;

private:
    Syn2CircuitHdf5 syn2_file_;

    const uint64_t total_records_;
    const string population_name_;
    uint64_t output_file_offset_;
};


void write_data(Syn2CircuitHdf5::Dataset& ds,
                uint64_t r_offset,
                const std::shared_ptr<const arrow::Column>& r_col_data);

inline hid_t parquet_types_to_h5(arrow::Type::type t);


} }  // NS EOF
#endif // CIRCUITWRITERSYN2_H
