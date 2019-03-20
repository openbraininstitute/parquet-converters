/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#include <stdio.h>
#include <unistd.h>
#include <string.h>

#include <neuron_parquet/touches.h>
#include <progress.hpp>


using namespace neuron_parquet::touches;

using neuron_parquet::Converter;
using utils::ProgressMonitor;

typedef Converter<IndexedTouch> TouchConverter;


enum class RunMode:int {QUIT_ERROR=-1, QUIT_OK, STANDARD};
struct Args {
    Args (RunMode runmode)
        : mode(runmode)
        , convert_limit(0)
        , n_opts(0)
    {}

    RunMode mode;
    int convert_limit;
    int n_opts;
};


static const char *usage =
    "usage: touch2parquet <touch_file1 touch_file2 ...>\n"
    "       touch2parquet [-h]\n";


Args process_args(int argc, char* argv[]) {
    if( argc < 2) {
        printf("%s", usage);
        return Args(RunMode::QUIT_ERROR);
    }

    Args args(RunMode::STANDARD);

    //Handle options
    int cur_opt = 1;
    for (; cur_opt < argc && argv[cur_opt][0] == '-'; cur_opt++ ) {
        switch( argv[cur_opt][1] ) {
            case 'h':
                printf("%s", usage);
                return Args(RunMode::QUIT_OK);
            case 'n':
                args.convert_limit = atoi(argv[cur_opt]+2);
                break;
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

    return args;
}



int main( int argc, char* argv[] ) {
    Args args = process_args(argc, argv);
    if( args.mode < RunMode::STANDARD ) {
        return static_cast<int>(args.mode);
    }

    int first_file = args.n_opts;
    int number_of_files = argc - first_file;

    // Know the total number of buffers to be processed
    uint32_t blocks;
    if (args.convert_limit > 0) {
        TouchReader tr(argv[first_file]);
        blocks = TouchConverter::number_of_buffers(args.convert_limit * tr.record_size());
    }
    else {
        std::ifstream f1(argv[first_file], std::ios::binary | std::ios::ate);
        blocks = TouchConverter::number_of_buffers(f1.tellg());
    }

    // Craete the progres monitor with an estimate on the number of blocks
    ProgressMonitor progress(number_of_files * blocks, true);
    progress.set_parallelism(0);

    #pragma omp parallel for
    for (int i=args.n_opts; i<argc; i++) {
        const char* in_filename = argv[i];
        progress.print_info("[Info] Converting %-86s\n", in_filename);

        TouchReader tr(in_filename);
        string parquetFilename( argv[i] );
        std::size_t slashPos = parquetFilename.find_last_of("/\\");
        parquetFilename = parquetFilename.substr(slashPos+1);
        parquetFilename += ".parquet";

        try {
            TouchWriterParquet tw(parquetFilename, tr.version());
            ProgressMonitor::SubTask subp = progress.subtask();
            TouchConverter converter(tr, tw);
            converter.setProgressHandler(subp);

            if( args.convert_limit > 0 )
                converter.exportN((unsigned) args.convert_limit);
            else {
                converter.exportAll();
            }
        }
        catch (const std::exception& e){
            printf("\n[ERROR] Could not create output file.\n -> %s\n", e.what());
        }
    }
    progress.clear(); // - this works well but
    printf("Done exporting\n");

    return 0;
}

