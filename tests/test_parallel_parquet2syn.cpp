#include "circuit_defs.h"
#include "circuit_reader_parquet.h"
#include "circuit_writer_syn2.h"
#include "converter.h"
#include "progress.h"
#include <iostream>
#include <vector>

#include <mpi.h>
#include <mpio.h>


using namespace neuron_parquet::circuit;

int mpi_size, mpi_rank;
MPI_Comm comm  = MPI_COMM_WORLD;
MPI_Info info  = MPI_INFO_NULL;


void convert_circuit(const std::vector<std::string>& filenames)  {

    // Each reader and each writer in a separate MPI process
    CircuitReaderParquet reader(filenames[mpi_rank]);
    uint64_t record_count = reader.record_count();

    uint64_t global_record_sum;
    MPI_Reduce(&record_count, &global_record_sum, 1, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);

    CircuitWriterSYN2 writer(std::string("circuit_syn2"), global_record_sum);
    writer.use_mpio();

    Converter<CircuitData> converter( reader, writer );

    if(mpi_rank==0) {
        ProgressMonitor p;
        converter.setProgressHandler(p.getNewHandler());
    }

    converter.exportAll();
    writer.close_files();
}

int main(int argc, char* argv[]) {
    // Initialize MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(comm, &mpi_size);
    MPI_Comm_rank(comm, &mpi_rank);

    if(argc < 2) {
        if(mpi_rank==0)
            std::cout << "Please provide at least a file to convert. Multiple files accepted";
        return -1;
    }
    if (mpi_size != argc-1) {
        if(mpi_rank==0)
            std::cout << "Please run with one MPI process per file to be converted";
        return -2;
    }

    std::vector<std::string> filenames(argc-1);
    for(int i=1; i<argc; i++) {
        filenames[i-1] = std::string(argv[i]);
    }
    convert_circuit(filenames);
    MPI_Finalize();
    return 0;
}
