#ifndef CIRCUITWRITERSYN2_H
#define CIRCUITWRITERSYN2_H


#include "generic_writer.h"
#include "circuit_defs.h"
#include "zeromem_queue.h"
#include <string>
#include <unordered_map>
#include <memory>

namespace neuron_parquet {
namespace circuit {


typedef ZeroMemQ<arrow::Column> ZeroMemQ_Column;

//debug
typedef int h5;

///
/// \brief The CircuitWriterSYN2 which writes one hdf5 file per column.
///        Each file is written by a separate thread, so the main thread only
///        dispatches data and is immediately freed to fetch more data.
///
class CircuitWriterSYN2 : public Writer<CircuitData>
{
public:
    CircuitWriterSYN2(const std::string & destination_dir);

    virtual void write(const CircuitData* data, uint length) override;

private:
    // The writer can write to several files using different threads
    // and therefore release the main thread to go and fetch more data

    void create_thread_process_data(h5 h5_file, ZeroMemQ_Column & queue);

    std::vector<ZeroMemQ_Column> column_writer_queues_;
    std::unordered_map<std::string, int> col_name_to_idx_;

    const std::string & destination_dir_;

};


// static
h5 init_h5file(const std::string & filename, std::shared_ptr<arrow::Column> column);


}
}

#endif // CIRCUITWRITERSYN2_H
