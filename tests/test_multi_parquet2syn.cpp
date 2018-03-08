#include "circuit_defs.h"
#include "circuit_reader_parquet.h"
#include "circuit_writer_syn2.h"
#include "converter.h"
#include "progress.h"
#include <vector>
#include <iostream>


using namespace neuron_parquet;
using namespace neuron_parquet::circuit;

using std::vector;
using std::string;

void convert_circuit(const vector<string> & filenames)  {
    CircuitMultiReaderParquet reader(filenames) ;
    CircuitWriterSYN2 writer(string("circuit_syn2"), reader.record_count());

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
        std::cout << "Please provide files to convert";
        return -1;
    }

    vector<string> names(argc-1);

    for(int i=1; i<argc; i++) {
        names[i-1] = string(argv[i]);
    }

    convert_circuit(names);
    return 0;
}
