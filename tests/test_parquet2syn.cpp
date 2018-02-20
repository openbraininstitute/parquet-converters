#include "circuit_defs.h"
#include "circuit_reader_parquet.h"
#include "circuit_writer_syn2.h"
#include "converter.h"
#include "progress.h"



using namespace neuron_parquet::circuit;


void convert_circuit(const std::string & filename)  {
    CircuitReaderParquet reader(filename) ;
    CircuitWriterSYN2 writer(std::string("output.parquet"), reader.record_count());

    Converter<CircuitData> converter( reader, writer );

    ProgressMonitor p;
    converter.setProgressHandler(p.getNewHandler());

    converter.exportAll();
}

int main(int argc, char* argv[]) {
    convert_circuit(std::string(argv[1]));
    return 0;
}
