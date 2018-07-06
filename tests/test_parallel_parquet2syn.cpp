#include <iostream>
#include <vector>
#include <unordered_map>
#include <mpi.h>
#include <syn2/synapses_writer.hpp>

#include "circuit/circuit_defs.h"
#include "circuit/parquet_reader.h"
#include "circuit/syn2_writer.h"
#include "converter.h"
#include "progress.h"


using namespace neuron_parquet;
using namespace neuron_parquet::circuit;
using std::string;


int mpi_size, mpi_rank;
MPI_Comm comm  = MPI_COMM_WORLD;
MPI_Info info  = MPI_INFO_NULL;



static const string syn2_filename ("circuit.syn2");


///
/// \brief convert_circuit: Converts parquet files to SYN2
///
void convert_circuit(const std::vector<string>& filenames)  {

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

    uint64_t *offsets=nullptr;
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

    CircuitWriterSYN2 writer(syn2_filename, global_record_sum, {comm, info}, offset);

    //Create converter and progress monitor

    Converter<CircuitData> converter( reader, writer );

    std::unique_ptr<ProgressMonitor> p;

    if(mpi_rank == 0) {
        p.reset(new ProgressMonitor(reader.block_count()));
        p->task_start(mpi_size);
    }

    // Progress handlers for worker nodes are just a function that triggers incrementing the progressbar
    auto f = [&p](int){
        if(mpi_rank == 0) {
            p->next();
        }
    };
    converter.setProgressHandler(f);

    converter.exportAll();

    if(mpi_rank == 0) {
        p->task_done(mpi_size);

        // Check for datasets with required name for SYN2
        Syn2CircuitHdf5& syn2circuit = writer.syn2_file();

        // SYN2 required fields might have a different name
        if(!syn2circuit.has_dataset("connected_neurons_pre")) {
            std::unordered_map<string, string> mapping {
                { string("pre_neuron_id"),  string("connected_neurons_pre")  },
                { string("post_neuron_id"), string("connected_neurons_post") },
                { string("pre_gid"),  string("connected_neurons_pre")  },
                { string("post_gid"), string("connected_neurons_post") },
            };

            for(auto map_pair : mapping) {
                if(syn2circuit.has_dataset(map_pair.first)) {
                    syn2circuit.link_dataset(map_pair.second, map_pair.first);
                }
            }
        }
    }

}



int main(int argc, char* argv[]) {
    // Initialize MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(comm, &mpi_size);
    MPI_Comm_rank(comm, &mpi_rank);

    if(argc < 2) {
        if(mpi_rank==0)
            std::cout << "Please provide at least a file to convert. Multiple files accepted" << std::endl;
        return -1;
    }
    if (mpi_size != argc-1) {
        if(mpi_rank==0)
            std::cout << "Please run with one MPI process per file to be converted" << std::endl;
        return -2;
    }

    std::vector<string> filenames(argc-1);
    for(int i=1; i<argc; i++) {
        filenames[i-1] = string(argv[i]);
    }

    // Bulk conversion with MPI
    convert_circuit(filenames);

    if(mpi_rank == 0) {
        std::cout << "\nData copy complete. Creating SYN2 indexes..." << std::endl;

        syn2::synapses_writer writer(syn2_filename, syn2::synapses_writer::use_mpi_flag);
        writer.create_all_index();

        std::cout << "Conversion finished." << std::endl;
    }


    MPI_Finalize();

    return 0;
}
