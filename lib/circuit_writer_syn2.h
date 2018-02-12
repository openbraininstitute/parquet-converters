#ifndef CIRCUITWRITERSYN2_H
#define CIRCUITWRITERSYN2_H

#include "generic_writer.h"
#include "circuit_defs.h"
#include <thread>
#include <string>
#include <unordered_map>

namespace neuron_parquet {
namespace circuit {

class CircuitWriterSYN2 : public Writer<CircuitData>
{
public:
    CircuitWriterSYN2();

    virtual void write(T* data, uint length) override;

private:
    // The writer can write to several files using different threads
    // and therefore release the main thread to go and fetch more data
    // NOTE: We assume all row groups have the same columns.


    std::shared_ptr<std::thread> create_thread_process_data(h5 h5_file, std::shared_ptr<const arrow::Column>& column);

    std::vector<std::shard_ptr<std::thread>> writer_threads;
    std::vector<std::shared_ptr<arrow::Column>> data_columns_to_process;

}


}
}

#endif // CIRCUITWRITERSYN2_H
