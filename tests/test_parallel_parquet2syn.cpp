#include "circuit_defs.h"
#include "circuit_reader_parquet.h"
#include "circuit_writer_syn2.h"
#include "converter.h"
#include "progress.h"
#include <iostream>
#include <vector>

#include <mpi.h>
#include <mpio.h>

using namespace neuron_parquet;
using namespace neuron_parquet::circuit;

int mpi_size, mpi_rank;
MPI_Comm comm  = MPI_COMM_WORLD;
MPI_Info info  = MPI_INFO_NULL;


void convert_circuit(const std::vector<std::string>& filenames)  {

    // Each reader and each writer in a separate MPI process

    CircuitReaderParquet reader(filenames[mpi_rank]);

    // Count the records and
    // 1. Sum
    // 2. Calculate offsets

    uint64_t record_count = reader.record_count();
    uint64_t global_record_sum;
    MPI_Allreduce(&record_count, &global_record_sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    uint32_t block_count = reader.block_count();
    uint32_t global_block_sum;
    MPI_Allreduce(&block_count, &global_block_sum, 1, MPI_UINT32_T, MPI_SUM, MPI_COMM_WORLD);



    uint64_t *offsets;
    if (mpi_rank == 0) {
        offsets = new uint64_t[mpi_size+1];
        offsets[0] = 0;
    }
    MPI_Gather(&record_count, 1, MPI_UINT64_T, offsets+1, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
    if (mpi_rank == 0) {
        for(int i=1; i<mpi_size; i++) {
            offsets[i] += offsets[i-1];
        }
    }

    uint64_t offset;
    MPI_Scatter(offsets, 1, MPI_UINT64_T, &offset, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    std::cout << "Process " << mpi_rank << " is gonna write on offset " << offset << std::endl;


    // Create writer

    CircuitWriterSYN2 writer(std::string("circuit_syn2"), global_record_sum);
    writer.use_mpio();
    writer.set_output_block_position(mpi_rank, offset, record_count);


    //Create converter and progress monitor

    Converter<CircuitData> converter( reader, writer );

    std::unique_ptr<ProgressMonitor> p;

    if(mpi_rank == 0) {
        p.reset(new ProgressMonitor(reader.block_count()));
    }

    // Progress handlers for worker nodes are just a function that participate in the MPI reduce
    // This has the additional eventually desirable effect of syncronizing block writes
    auto f = [&p](float progress){
        float global_progress;
        MPI_Reduce(&progress, &global_progress, 1, MPI_FLOAT, MPI_SUM, 0, MPI_COMM_WORLD);
        if(mpi_rank == 0) {
            p->updateProgress(1);
        }

    };
    converter.setProgressHandler(f);

    converter.exportAll();

    if(mpi_rank == 0) {
        std::cout << "\nComplete." << std::endl;
    }

    // Sync before destroying readers, writers
    MPI_Barrier(comm);
}


int main(int argc, char* argv[]) {
    // Initialize MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(comm, &mpi_size);
    MPI_Comm_rank(comm, &mpi_rank);

    if(argc < 2) {
        if(mpi_rank==0)
            std::cout << "Please provide at least a file to convert. Multiple files accepted" << endl;
        return -1;
    }
    if (mpi_size != argc-1) {
        if(mpi_rank==0)
            std::cout << "Please run with one MPI process per file to be converted" << endl;
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
