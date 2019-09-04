/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#include <stdexcept>
#include <iomanip>
#include <iostream>
#include <vector>
#include <unordered_map>
#ifdef NEURONPARQUET_USE_MPI
#include <mpi.h>
#endif

#include "CLI/CLI.hpp"

#include <syn2/synapses_writer.hpp>

#include <neuron_parquet/circuit.h>
#include <progress.hpp>


using namespace neuron_parquet::circuit;

using neuron_parquet::Converter;
using utils::ProgressMonitor;
using std::string;
using std::cout;

enum class Output : int { SYN2, SONATA, Hybrid };


int mpi_size, mpi_rank;
#ifdef NEURONPARQUET_USE_MPI
MPI_Comm comm = MPI_COMM_WORLD;
MPI_Info info = MPI_INFO_NULL;
#endif



///
/// \brief convert_circuit: Converts parquet files to SYN2
///
void convert_circuit(const std::vector<string>& filenames, const string& syn2_filename)  {

    // Each reader and each writer in a separate MPI process

    CircuitMultiReaderParquet reader(filenames);

#ifdef NEURONPARQUET_USE_MPI
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
              << " block(s) with an offset of " << std::setw(12) << offset
              << std::endl;

    CircuitWriterSYN2 writer(syn2_filename, global_record_sum, {comm, info}, offset);
#else
    cout << "Aggregate totals: "
         << reader.record_count() << " records ("
         << reader.block_count() << " blocks)"
         << std::endl;

    CircuitWriterSYN2 writer(syn2_filename, reader.record_count());
#endif


    //Create converter and progress monitor
    {
        Converter<CircuitData> converter(reader, writer);
#ifdef NEURONPARQUET_USE_MPI
        ProgressMonitor p(global_block_sum, mpi_rank==0);

        // Use progress of first process to estimate global progress
        if (mpi_rank == 0) {
            p.set_parallelism(mpi_size);
            converter.setProgressHandler(p, mpi_size);
        }
#else
        ProgressMonitor p(reader.block_count());

        converter.setProgressHandler(p);
#endif

        converter.exportAll();
    }

#ifdef NEURONPARQUET_USE_MPI
    MPI_Barrier(comm);
#endif

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


int main(int argc, char* argv[]) {
#ifdef NEURONPARQUET_USE_MPI
    // Initialize MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(comm, &mpi_size);
    MPI_Comm_rank(comm, &mpi_rank);
#else
    mpi_size = 1;
    mpi_rank = 0;
#endif

    Output format = Output::Hybrid;
    std::vector<std::pair<std::string, Output>> map{
        {"syn2", Output::SYN2},
        {"sonata", Output::SONATA},
        {"hybrid", Output::Hybrid}
    };
    string output_filename("circuit.syn2");
    std::vector<string> all_input_names;

    // Every node makes his job in reading the args and
    // compute the sub array of files to process
    CLI::App app{"Convert Parquet synapse files into HDF5 formats"};
    app.add_option("-o", output_filename, "Specify the output filename");
    app.add_option("-f,--format", format, "Format of the output file contents")
        ->transform(CLI::CheckedTransformer(map, CLI::ignore_case));
    app.add_option("files", all_input_names, "Files to convert")
        ->expected(-1);

    try {
        app.parse(argc, argv);
    } catch(const CLI::ParseError& e) {
        if (mpi_rank == 0) {
            app.exit(e);
        }
#ifdef NEURONPARQUET_USE_MPI
        MPI_Finalize();
#endif
        return 1;
    }

#ifdef NEURONPARQUET_USE_MPI
    int total_files = all_input_names.size();
    int my_n_files = total_files / mpi_size;
    int remaining = total_files % mpi_size;
    if( mpi_rank < remaining ) {
        my_n_files++;
    }

    int my_offset = total_files / mpi_size * mpi_rank;
    my_offset += ( mpi_rank > remaining )? remaining : mpi_rank;

    std::vector<string> input_names(all_input_names.begin()+my_offset, all_input_names.begin() + my_offset + my_n_files);
    cout << std::setfill('.')
         << "Process " << std::setw(4) << mpi_rank
         << " is going to read files " << std::setw(8) << my_offset
         << " to " << std::setw(8) << my_offset + my_n_files << std::endl;

    MPI_Barrier(comm);

    if (mpi_rank == 0) {
        std::cout << "Writing to " << output_filename << std::endl;
    }

    MPI_Barrier(comm);
#else
    std::cout << "Writing to " << output_filename << std::endl;
    const auto input_names = all_input_names;
#endif

    convert_circuit(input_names, output_filename);

#ifdef NEURONPARQUET_USE_MPI
    MPI_Barrier(comm);
#endif

    if(mpi_rank == 0) {
        cout << std::endl
             << "Data conversion complete. " << std::endl
             << "Creating SYN2 indexes..." << std::endl;
    }

#ifdef NEURONPARQUET_USE_MPI
    MPI_Barrier(comm);
#endif

    {
#ifdef NEURONPARQUET_USE_MPI
        syn2::synapses_writer writer(output_filename, syn2::synapses_writer::use_mpi_flag);
#else
        syn2::synapses_writer writer(output_filename);
#endif
        writer.create_all_index();
        if (format == Output::SONATA or format == Output::Hybrid) {
            writer.compose_sonata(format == Output::SONATA);
        }
    }

#ifdef NEURONPARQUET_USE_MPI
    MPI_Barrier(comm);
#endif

    if(mpi_rank == 0) {
        cout << "Finished." << std::endl;
    }

#ifdef NEURONPARQUET_USE_MPI
    MPI_Finalize();
#endif

    return 0;
}
