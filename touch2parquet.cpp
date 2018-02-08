#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include "touch_reader.h"
#include "touch_writer_parquet.h"
#include "converter.h"
#include "progress.h"

enum class RunMode:int { QUIT_ERROR=-1, QUIT_OK, STANDARD, ENDIAN_SWAP };
struct Args {
    Args (RunMode mode)
        : mode(mode) {}
    RunMode mode;
    int convert_limit = 0;
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

    //It will swap endians if we use the xxx_endian executable
    int len = strlen(argv[0]);
    if( len>6 && strcmp( &(argv[0][len-6]), "endian" ) == 0 ) {
        printf("[Info] Swapping endians\n");
        args.mode = RunMode::ENDIAN_SWAP;
    }

    return args;
}


int main( int argc, char* argv[] ) {
    Args args = process_args(argc, argv);
    if( args.mode < RunMode::STANDARD ) {
        return static_cast<int>(args.mode);
    }

    ProgressMonitor progress(argc-args.n_opts);

    #pragma omp parallel for
    for( int i=args.n_opts; i<argc; i++) {
        printf("\r[Info] Converting %-86s\n", argv[i]);
        TouchReader tr(argv[i], args.mode == RunMode::ENDIAN_SWAP);
        string parquetFilename( argv[i] );
        std::size_t slashPos = parquetFilename.find_last_of("/\\");
        parquetFilename = parquetFilename.substr(slashPos+1);
        parquetFilename += ".parquet";

        try {
            TouchWriterParquet tw(parquetFilename);
            Converter<Touch> converter(tr, tw);
            converter.setProgressHandler(progress.getNewHandler());
            if( args.convert_limit > 0 )
                converter.exportN((unsigned) args.convert_limit);
            else {
                converter.exportAll();
            }
            progress.task_done(i-args.n_opts);
        }
        catch (const std::exception& e){
            printf("\n[ERROR] Could not create output file.\n -> %s", e.what());
        }
    }

    printf("\nDone exporting\n");
    return 0;
}


