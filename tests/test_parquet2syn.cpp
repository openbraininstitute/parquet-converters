#include <iostream>
#include "circuit/circuit_defs.h"
#include "circuit/parquet_reader.h"
#include "circuit/syn2_writer.h"
#include "converter.h"
#include "progress.h"


using namespace neuron_parquet;
using namespace neuron_parquet::circuit;


void convert_circuit(const std::string & filename)  {
    CircuitReaderParquet reader(filename) ;
    CircuitWriterSYN2 writer(std::string("circuit_syn2"), reader.record_count());

    Converter<CircuitData> converter( reader, writer );

    ProgressMonitor p(reader.block_count());
    converter.setProgressHandler(p.getNewHandler());

    p.task_start();
    converter.exportAll();
    p.task_done();

    std::cout << "\nComplete." << std::endl;
}

int main(int argc, char* argv[]) {
    if(argc < 2) {
        std::cout << "Please provide a file to convert";
        return -1;
    }
    convert_circuit(std::string(argv[1]));
    return 0;
}
