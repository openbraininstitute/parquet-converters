#ifndef CIRCUITWRITERSYN2_H
#define CIRCUITWRITERSYN2_H


#include "generic_writer.h"
#include "circuit_defs.h"
#include "zeromem_queue.h"
#include <string>
#include <unordered_map>
#include <memory>
#include <hdf5.h>

namespace neuron_parquet {
namespace circuit {


typedef ZeroMemQ<arrow::Column> ZeroMemQ_Column;

struct h5_ids {
    hid_t file, ds, dspace;
};


///
/// \brief The CircuitWriterSYN2 which writes one hdf5 file per column.
///        Each file is written by a separate thread, so the main thread only
///        dispatches data and is immediately freed to fetch more data.
///
class CircuitWriterSYN2 : public Writer<CircuitData>
{
public:
    CircuitWriterSYN2(const std::string & destination_dir, uint64_t n_records);

    virtual void write(const CircuitData* data, uint length) override;

private:
    // The writer can write to several files using different threads
    // and therefore release the main thread to go and fetch more data

    h5_ids init_h5file(const std::string & filename, std::shared_ptr<arrow::Column> column);
    void create_thread_process_data(h5_ids h5_output, ZeroMemQ_Column & queue);

    std::vector<ZeroMemQ_Column> column_writer_queues_;
    std::unordered_map<std::string, int> col_name_to_idx_;

    const std::string & destination_dir_;
    const uint64_t total_records_;


};


// Thread functions

inline void write_data(h5_ids h5_ds, uint64_t& offset, const std::shared_ptr<const arrow::Column>& col_data);
inline hid_t parquet_types_to_h5(arrow::Type::type t);


} }  // NS EOF
#endif // CIRCUITWRITERSYN2_H
