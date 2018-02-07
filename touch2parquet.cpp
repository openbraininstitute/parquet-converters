#include <stdio.h>
#include <unistd.h>
#include <functional>
#include <vector>
#include <numeric>
#include "touch_reader.h"
#include "touch_writer_parquet.h"
#include "converter.h"
#include "progress.h"


enum RunMode { QUIT_ERROR=-1, QUIT_OK, STANDARD, ENDIAN_SWAP };
RunMode process_args(int argc, char* argv[]);


int main( int argc, char* argv[] ) {
    RunMode mode = process_args(argc, argv);
    if(mode < STANDARD) {
        return mode;
    }

    ProgressMonitor progress(argc-1);

    #pragma omp parallel for
    for( int i=1; i<argc; i++) {
        printf("\r[Info] Converting %-86s\n", argv[i]);
        TouchReader tr(argv[i], mode == ENDIAN_SWAP);
        string parquetFilename( argv[i] );
        std::size_t slashPos = parquetFilename.find_last_of("/\\");
        parquetFilename = parquetFilename.substr(slashPos+1);
        parquetFilename += ".parquet";

        try {
            TouchWriterParquet tw(parquetFilename);
            Converter<Touch> converter(tr, tw);
            converter.setProgressHandler(progress.getNewHandler());
            converter.exportAll();
        }
        catch (const std::exception& e){
            printf("\n[ERROR] Could not create output file.\n -> %s", e.what());
        }
    }

    printf("\nDone exporting\n");
    return 0;
}


RunMode process_args(int argc, char* argv[]) {

    if( argc < 2) {
        printf("usage: touch2parquet[_endian] <touch_file1 touch_file2 ...>\n");
        printf("       touch2parquet [-h]\n");
        return QUIT_ERROR;
    }
    if ( argv[1][0] == '-' ) {
        //Handle options, especially -h, --help
        // For the moment there are no options. We only show usage and quit.
        printf("usage: touch2parquet[_endian] <touch_file1 touch_file2 ...>\n");
        printf("       touch2parquet [-h]\n");
        return QUIT_OK;
    }

    bool arg_error = false;
    for( int i=1; i<argc; i++) {
        if(access(argv[i] , F_OK) == -1) {
            fprintf(stderr, "File '%s' doesn't exist, or no permission\n", argv[i]);
            arg_error = true;
        }
    }
    if(arg_error) {
        return QUIT_ERROR;
    }

    //It will swap endians if we use the xxx_endian executable
    int len = strlen(argv[0]);
    if( len>6 && strcmp( &(argv[0][len-6]), "endian" ) == 0 ) {
        printf("[Info] Swapping endians\n");
        return ENDIAN_SWAP;
    }

    return STANDARD;
}
