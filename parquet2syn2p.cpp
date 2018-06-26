#include "circuit_defs.h"
#include "circuit_reader_parquet.h"
#include "circuit_writer_syn2.h"
#include "converter.h"
#include "progress.h"
#include "syn2_circuit.h"
#include <syn2/synapses_writer.hpp>

#include <stdexcept>
#include <iomanip>
#include <iostream>
#include <vector>
#include <unordered_map>
#include <mpi.h>


using namespace neuron_parquet;
using namespace neuron_parquet::circuit;
using std::string;
using std::cout;


int mpi_size, mpi_rank;
MPI_Comm comm  = MPI_COMM_WORLD;
MPI_Info info  = MPI_INFO_NULL;



///
/// \brief convert_circuit: Converts parquet files to SYN2
///
void convert_circuit(const std::vector<string>& filenames, const string& syn2_filename)  {

    // Each reader and each writer in a separate MPI process

    CircuitMultiReaderParquet reader(filenames);

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

    std::cout << std::setfill('.')
              << "Process " << std::setw(4) << mpi_rank
              << " is going to write " << std::setw(12) << reader.block_count()
              << " block(s) with an offset of " << std::setw(12) << offset << std::endl;

    // Create writer
    CircuitWriterSYN2 writer(syn2_filename, global_record_sum, {comm, info}, offset);


    //Create converter and progress monitor

    Converter<CircuitData> converter( reader, writer );

    ProgressMonitor p(global_block_sum);

    //Only the first shows progress
    if(mpi_rank == 0) {
        p.task_start(mpi_size);
        // Progress handlers is just a function that triggers incrementing the progressbar
        auto f = [&p](int){
            if(mpi_rank == 0) {
                p.updateProgress(mpi_size);
            }
        };
        converter.setProgressHandler(f);
    }

    converter.exportAll();

    MPI_Barrier(comm);

    if(mpi_rank == 0) {
        p.task_done(mpi_size);
    }

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


void parse_arguments(const int argc, char* argv[], string& output_filename, std::vector<string>& input_names)  {
    for(int i=1; i<argc; i++) {
        if(argv[i][0] != '-') {
            input_names.push_back(string(argv[i]));
        }
        else{
            switch( argv[i][1] ) {
                case 'o': {
                    const char* p_name = argv[i]+2;
                    if( p_name[0] == 0 ) {
                        // Option value separated
                        if(++i == argc)  {
                            throw std::runtime_error("Please provide an argment to -o");
                        }
                        p_name = argv[i];
                    }
                    output_filename = p_name;
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

    string output_filename("circuit.syn2");
    std::vector<string>
            all_input_names;

    // Every node makes his job in reading the args and
    // compute the sub array of files to process

    try  {
        parse_arguments(argc, argv, output_filename, all_input_names);
        if(all_input_names.size() == 0) {
            throw std::runtime_error("Please provide at least one file to convert");
        }
    }
    catch(const std::exception& e) {
        std::cout << "Arguments error: " << e.what() << std::endl
                  << "Usage: parquet2syn2p [-o outfile] file..." << std::endl;
        MPI_Finalize();
        return -1;
    }

    int total_files = all_input_names.size();
    int my_n_files = total_files / mpi_size;
    int remaining = total_files % mpi_size;
    if( mpi_rank < remaining ) {
        my_n_files++;
    }

    int my_offset = total_files / mpi_size * mpi_rank;
    my_offset += ( mpi_rank > remaining )? remaining : mpi_rank;

    std::vector<string> my_input_names(all_input_names.begin()+my_offset, all_input_names.begin() + my_offset + my_n_files);
    cout << std::setfill('.')
         << "Process " << std::setw(4) << mpi_rank
         << " is going to read files " << std::setw(8) << my_offset
         << " to " << std::setw(8) << my_offset + my_n_files << std::endl;

    MPI_Barrier(comm);

    if (mpi_rank == 0)
        std::cout << "Writing to " << output_filename << std::endl;

    MPI_Barrier(comm);

    // Bulk conversion with MPI
    convert_circuit(my_input_names, output_filename);

    MPI_Barrier(comm);

    if(mpi_rank == 0) {
        cout << std::endl
             << "Data conversion complete. " << std::endl
             << "Creating SYN2 indexes..." << std::endl;
    }

    MPI_Barrier(comm);

    {
        syn2::synapses_writer writer(output_filename, syn2::synapses_writer::use_mpi_flag);
        writer.create_all_index();
    }

    MPI_Barrier(comm);

    if(mpi_rank == 0) {
        cout << "Finished." << std::endl;
    }

    MPI_Finalize();

    return 0;
}
