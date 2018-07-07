/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Matthias Wolf <matthias.wolf@epfl.ch>
 *
 */
#include <cmath>
#include <boost/filesystem.hpp>
#include <stdio.h>
#include <mpi.h>
#include <unistd.h>
#include <string.h>

#include <neuron_parquet/touches.h>
#include <progress.hpp>


using namespace neuron_parquet;

// Stand-in until c++ std::filesystem is supported
namespace fs = boost::filesystem;

typedef Converter<Touch> TouchConverter;


int mpi_size, mpi_rank;
MPI_Comm comm = MPI_COMM_WORLD;
MPI_Info info = MPI_INFO_NULL;


enum class RunMode:int { QUIT_ERROR=-1, QUIT_OK, STANDARD, ENDIAN_SWAP };
struct Args {
    Args (RunMode runmode)
    : mode(runmode)
    {}
    RunMode mode;
    int n_opts = 0;
};


static const char *usage =
    "usage: touch2parquet[_endian] <touch_file1 touch_file2 ...>\n"
    "       touch2parquet [-h]\n";


Args process_args(int argc, char* argv[]) {
    if( argc < 2) {
        printf("%s", usage);
        return Args(RunMode::QUIT_ERROR);
    }

    Args args(RunMode::STANDARD);
    int cur_opt = 1;

    //Handle options
    for( ;cur_opt < argc && argv[cur_opt][0] == '-'; cur_opt++ ) {
        switch( argv[cur_opt][1] ) {
            case 'h':
                printf("%s", usage);
                return Args(RunMode::QUIT_OK);
        }
    }
    args.n_opts = cur_opt;


    for( int i=cur_opt ; i<argc ; i++ ) {
        if(access(argv[i] , F_OK) == -1) {
            fprintf(stderr, "File '%s' doesn't exist, or no permission\n", argv[i]);
            args.mode = RunMode::QUIT_ERROR;
            return args;
        }
    }

    //It will swap endians if we use the xxx_endian executable
    int len = strlen(argv[0]);
    if( len>6 && strcmp( &(argv[0][len-6]), "endian" ) == 0 ) {
        printf("[Info] Swapping endians\n");
        args.mode = RunMode::ENDIAN_SWAP;
    }

    return args;
}



int main( int argc, char* argv[] ) {
    MPI_Init(&argc, &argv);
    MPI_Comm_size(comm, &mpi_size);
    MPI_Comm_rank(comm, &mpi_rank);

    Args args = process_args(argc, argv);
    if( args.mode < RunMode::STANDARD ) {
        return static_cast<int>(args.mode);
    }

    std::string first_file(argv[args.n_opts]);
    int number_of_files = argc - args.n_opts;

    size_t nblocks = 0;
    if (mpi_rank == 0) {
        // Know the total number of buffers to be processed
        std::ifstream f1(first_file, std::ios::binary | std::ios::ate);
        nblocks = TouchConverter::number_of_buffers(f1.tellg());
        f1.close();
    }


    // Progress with an estimate number of blocks
    ProgressMonitor progress(number_of_files * nblocks, mpi_rank==0);
    progress.set_parallelism(mpi_size);

    auto outfn = fs::path(first_file).stem().string() + "."
                 + std::to_string(mpi_rank) + ".parquet";
    try {
        TouchWriterParquet tw(outfn);

        for (int i = args.n_opts; i < argc; i++) {
            MPI_Barrier(comm);
            if (mpi_rank == 0)
                printf("\r[Info] Converting %-86s\n", argv[i]);

            TouchReader tr(argv[i], args.mode == RunMode::ENDIAN_SWAP);
            auto work_unit = static_cast<size_t>(std::ceil(tr.record_count() / double(mpi_size)));
            auto offset = work_unit * mpi_rank;
            work_unit = std::min(tr.record_count() - offset, work_unit);

            TouchConverter converter(tr, tw);
            if (mpi_rank == 0) {
                // Progress handlers is just a function that triggers incrementing the progressbar
                converter.setProgressHandler([&progress](int) {
                    progress += mpi_size;
                });
            }
            converter.exportN(work_unit, offset);
        }
    }
    catch (const std::exception& e){
        printf("\n[ERROR] Could not create output file for rank %d.\n -> %s\n", mpi_rank, e.what());
        MPI_Finalize();
        return 1;
    }

    MPI_Barrier(comm);
    MPI_Finalize();

    if (mpi_rank == 0)
        printf("\nDone exporting\n");
    return 0;
}


