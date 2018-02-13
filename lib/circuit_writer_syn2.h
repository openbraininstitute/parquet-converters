#ifndef CIRCUITWRITERSYN2_H
#define CIRCUITWRITERSYN2_H


#include "generic_writer.h"
#include "circuit_defs.h"
#include <thread>
#include <mutex>
#include <condition_variable>
#include <string>
#include <unordered_map>

namespace neuron_parquet {
namespace circuit {

//debug
typedef int h5;


class CircuitWriterSYN2 : public Writer<CircuitData>
{
public:
    CircuitWriterSYN2();

    virtual void write(CircuitData* data, uint length) override;

private:
    // The writer can write to several files using different threads
    // and therefore release the main thread to go and fetch more data
    // NOTE: We assume all row groups have the same columns.


    const std::shared_ptr<std::thread> create_thread_process_data(h5 h5_file, std::shared_ptr<arrow::Column>& r_column);

    std::vector<std::shared_ptr<std::thread>> writer_threads;
    std::vector<std::shared_ptr<arrow::Column>> data_columns_to_process;
    std::unordered_map<std::string, int> col_name_to_idx;
};


// static
h5 init_h5file(const std::string & filename, std::shared_ptr<arrow::Column> column);


}
}

#endif // CIRCUITWRITERSYN2_H
