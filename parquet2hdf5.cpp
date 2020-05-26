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

#include <bbp/sonata/nodes.h>
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
/// \brief link_ids Links the neuron ids to the right names
///
///
void link_ids(Syn2CircuitHdf5& syn2circuit) {
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

///
/// \brief convert_circuit Converts parquet files to SYN2, single-threaded
///
///
void convert_circuit(const std::vector<string>& filenames, const string& syn2_filename, const string& population)  {
    std::cout << "Writing to " << syn2_filename << std::endl;
    CircuitMultiReaderParquet reader(filenames);

    cout << "Aggregate totals: "
         << reader.record_count() << " records ("
         << reader.block_count() << " blocks)"
         << std::endl;

    CircuitWriterSYN2 writer(syn2_filename, reader.record_count(), population);

    //Create converter and progress monitor
    {
        Converter<CircuitData> converter(reader, writer);
        ProgressMonitor p(reader.block_count());
        converter.setProgressHandler(p);
        converter.exportAll();
    }

    // Check for datasets with required name for SYN2
    link_ids(writer.syn2_file());
}


///
/// \brief convert_circuit_mpi: Converts parquet files to SYN2 using mpi
///
///
void convert_circuit_mpi(const std::vector<string>& filenames, const string& syn2_filename, const string& population)  {

    // Each reader and each writer in a separate MPI process
    int total_files = filenames.size();
    int my_n_files = total_files / mpi_size;
    int remaining = total_files % mpi_size;
    if( mpi_rank < remaining ) {
        my_n_files++;
    }

    int my_offset = total_files / mpi_size * mpi_rank;
    my_offset += (mpi_rank > remaining) ? remaining : mpi_rank;

    std::vector<string> input_names(filenames.begin() + my_offset, filenames.begin() + my_offset + my_n_files);
    if (mpi_rank < filenames.size()) {
        cout << std::setfill('.')
             << "Process " << std::setw(4) << mpi_rank
             << " is going to read files " << std::setw(8) << my_offset
             << " to " << std::setw(8) << my_offset + my_n_files - 1 << std::endl;
    } else {
        cout << std::setfill('.')
             << "Process " << std::setw(4) << mpi_rank
             << " is not going to read files." << std::endl;
    }

    if (input_names.empty()) {
        // We need this to grab the schema of the input files. All ranks
        // need to have the schema to keep the state of the output HDF5
        // file in sync, otherwise the execution will hang when closing the
        // output HDF5 file.
        input_names.push_back(filenames.back());
    }

    MPI_Barrier(comm);

    if (mpi_rank == 0) {
        std::cout << "Writing to " << syn2_filename << std::endl;
    }

    MPI_Barrier(comm);

    CircuitMultiReaderParquet reader(input_names);

    // Count the records and
    // 1. Sum
    // 2. Calculate offsets

    uint64_t record_count = mpi_rank < filenames.size() ? reader.record_count() : 0;
    uint64_t global_record_sum;
    MPI_Allreduce(&record_count, &global_record_sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    uint32_t block_count = mpi_rank < filenames.size() ? reader.block_count() : 0;
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

    if (mpi_rank < filenames.size()) {
        std::cout << std::setfill('.')
                  << "Process " << std::setw(4) << mpi_rank
                  << " is going to write " << std::setw(12) << reader.block_count()
                  << " block(s) with an offset of " << std::setw(12) << offset
                  << std::endl;
    } else {
        std::cout << std::setfill('.')
                  << "Process " << std::setw(4) << mpi_rank
                  << " is not going to write anything."
                  << std::endl;
    }

    CircuitWriterSYN2 writer(syn2_filename, global_record_sum, {comm, info}, offset, population);

    //Create converter and progress monitor
    {
        Converter<CircuitData> converter(reader, writer);
        ProgressMonitor p(global_block_sum, mpi_rank==0);
        // Use progress of first process to estimate global progress
        if (mpi_rank == 0) {
            p.set_parallelism(mpi_size);
            converter.setProgressHandler(p, mpi_size);
        }
        if (mpi_rank < filenames.size()) {
            // See above: avoid converting data if we just opened the last
            // file to access the schema.
            converter.exportAll();
        }
    }

    MPI_Barrier(comm);

    // Check for datasets with required name for SYN2
    link_ids(writer.syn2_file());
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
    string output_filename;
    string output_population("default");
    std::vector<string> input_names;

    std::vector<std::string> source_population;
    std::vector<std::string> target_population;

    size_t count_pre = 0;
    size_t count_post = 0;

    // Every node makes his job in reading the args and
    // compute the sub array of files to process
    CLI::App app{"Convert Parquet synapse files into HDF5 formats"};
    app.add_option("-o", output_filename, "Specify the output filename");
    app.add_option("-p,--population", output_population, "Specify the output population to use");
    app.add_option("--from", source_population, "Node source population file and population name")
        ->expected(2);
    app.add_option("--to", target_population, "Node target population file and population name")
        ->expected(2);
    app.add_option("-f,--format", format, "Format of the output file contents")
        ->transform(CLI::CheckedTransformer(map, CLI::ignore_case));
    app.add_option("files", input_names, "Files to convert")
        ->required()
        ->check(CLI::ExistingFile);

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

    if (output_filename.empty()) {
        output_filename = (format == Output::SONATA)
                          ? "circuit.sonata"
                          : "circuit.syn2";
    }

    if (not source_population.empty()) {
        count_pre =
            bbp::sonata::NodeStorage(source_population[0])
            .openPopulation(source_population[1])
            ->size();
    }

    if (not target_population.empty()) {
        count_post =
            bbp::sonata::NodeStorage(target_population[0])
            .openPopulation(target_population[1])
            ->size();
    }

#ifdef NEURONPARQUET_USE_MPI
    if (mpi_size > 1) {
        convert_circuit_mpi(input_names, output_filename, output_population);
        MPI_Barrier(comm);
    } else {
#endif
    convert_circuit(input_names, output_filename, output_population);
#ifdef NEURONPARQUET_USE_MPI
    }
#endif

    if(mpi_rank == 0) {
        cout << std::endl
             << "Data conversion complete. " << std::endl
             << "Creating indices..." << std::endl;
    }

    {
#ifdef NEURONPARQUET_USE_MPI
        MPI_Barrier(comm);
        syn2::synapses_writer writer(output_filename, syn2::synapses_writer::use_mpi_flag);
#else
        syn2::synapses_writer writer(output_filename);
#endif
        writer.select_population(output_population);
        writer.create_all_index(count_pre, count_post);
        if (format == Output::SONATA or format == Output::Hybrid) {
            writer.compose_sonata(
                format == Output::SONATA,
                source_population.empty() ? "unknown" : source_population[1],
                target_population.empty() ? "unknown" : target_population[1]
            );
        }
#ifdef NEURONPARQUET_USE_MPI
        MPI_Barrier(comm);
#endif
    }

    if(mpi_rank == 0) {
        cout << "Finished writing " << output_filename << std::endl;
    }

#ifdef NEURONPARQUET_USE_MPI
    MPI_Finalize();
#endif

    return 0;
}
