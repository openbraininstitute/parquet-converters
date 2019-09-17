/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Matthias Wolf <matthias.wolf@epfl.ch>
 *
 */
#include <unistd.h>
#include <cmath>
#include <boost/filesystem.hpp>
#include <cstdio>
#include <cstring>
#include <mpi.h>

#include "CLI/CLI.hpp"

#include <neuron_parquet/touches.h>
#include <progress.hpp>

// Stand-in until c++ std::filesystem is supported
namespace fs = boost::filesystem;

using namespace neuron_parquet::touches;

using neuron_parquet::Converter;
using utils::ProgressMonitor;

typedef Converter<IndexedTouch> TouchConverter;


int mpi_size, mpi_rank;
#ifdef NEURONPARQUET_USE_MPI
MPI_Comm comm = MPI_COMM_WORLD;
#endif

int main( int argc, char* argv[] ) {

#ifdef NEURONPARQUET_USE_MPI
    //Initialize MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(comm, &mpi_size);
    MPI_Comm_rank(comm, &mpi_rank);
#else
    mpi_size=1;
    mpi_rank=0;
#endif

    //Parsing command line
    std::vector<std::string> all_input_names;
    std::string output_filename;
    int convert_limit;
    CLI::App app{"Convert TouchDetector output to Parquet synapse files"};
    app.add_option("-o", output_filename, "Specify the output filename");
    app.add_option("-n", convert_limit, "Maximum number of records to export");
    app.add_option("files", all_input_names, "Files to convert")
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

    std::string first_file(all_input_names[0]);
    int number_of_files = all_input_names.size();

    // Progress with an estimate number of blocks
    size_t nblocks = 1;
    if (mpi_rank == 0) {
      if (convert_limit >0) {
        TouchReader tr(first_file.c_str());
        nblocks = TouchConverter::number_of_buffers(convert_limit * tr.record_size());
      }
      else {
        std::ifstream f1(first_file, std::ios::binary | std::ios::ate);
        nblocks = TouchConverter::number_of_buffers(f1.tellg());
        f1.close();
      }
    }
    ProgressMonitor progress(number_of_files * nblocks, mpi_rank==0);
    progress.set_parallelism(mpi_size);

    if (output_filename.empty()) {
      output_filename = first_file;
    }
    auto outfn = fs::path(output_filename).stem().string() + "."
                 + std::to_string(mpi_rank) + ".parquet";

    try {
        auto version = TouchReader(first_file.c_str()).version();

        // Every rank participates in the conversion of every file, different regions
        TouchWriterParquet tw(outfn, version);

        for (int i = 0; i < number_of_files; i++) {
#ifdef NEURONPARQUET_USE_MPI
            MPI_Barrier(comm);
#endif
            const char* in_filename = all_input_names[i].c_str();

            if (mpi_rank == 0)
                printf("\r[Info] Converting %-86s\n", in_filename);

            TouchReader tr(in_filename);
            auto work_unit = static_cast<size_t>(std::ceil(tr.record_count() / double(mpi_size)));
            if (convert_limit > 0) {
                work_unit = static_cast<size_t>(std::ceil(convert_limit/double(mpi_size)));
            }
            auto offset = work_unit * mpi_rank;
            work_unit = std::min(tr.record_count() - offset, work_unit);

            TouchConverter converter(tr, tw);
            if (mpi_rank == 0) {
                // Progress handlers is just a function that triggers incrementing the progressbar
                converter.setProgressHandler(progress, mpi_size);
            }

            converter.exportN(work_unit, offset);
        }
    }
    catch (const std::exception& e){
#ifdef NEURONPARQUET_USE_MPI
        printf("\n[ERROR] Could not create output file for rank %d.\n -> %s\n", mpi_rank, e.what());
        MPI_Finalize();
        return 1;
#else
        printf("\n[ERROR] Could not create output file \n -> %s\n", mpi_rank, e.what());
        return 1;
#endif
    }

#ifdef NEURONPARQUET_USE_MPI
    MPI_Barrier(comm);
    MPI_Finalize();
#endif

    if (mpi_rank == 0)
        printf("\nDone exporting\n");
    return 0;
}


