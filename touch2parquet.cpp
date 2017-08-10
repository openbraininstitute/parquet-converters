#include <stdio.h>
#include <functional>
#include <vector>
#include <numeric>
#include "loader.h"
#include "parquetwriter.h"



int main( int argc, char* argv[] ) {

    if( argc < 2) {
        printf("usage: %s <touch_files ...>\n", argv[0]);
        return 1;
    }

    //It will swap endians if we use the xxx_endian executable
    int len = strlen(argv[0]);
    bool swap_endians = false;
    if( strcmp( &(argv[0][len-6]), "endian" ) == 0 ) {
        swap_endians=true;
        printf("[Info] Swapping endians\n");
    }

    ProgressHandler progress(argc-1);

    #pragma omp parallel for
    for( int i=1; i<argc; i++) {
        printf("\n[Info] Loading %s\n", argv[i]);
        Loader tl(argv[i], swap_endians);
        string parquetFilename( argv[i] );
        std::size_t slashPos = parquetFilename.find_last_of("/\\");
        parquetFilename = parquetFilename.substr(slashPos+1);
        parquetFilename += ".parquet";

        try {
            ParquetWriter tw(parquetFilename);
            tl.setExporter( tw );
            tl.setProgressHandler( progress.addSubTask() );
            tl.exportAll();
        }
        catch (const std::exception& e){
            printf("[ERROR] Could not create output file.\n -> %s", e.what());
        }
    }

    printf("Done exporting\n");
    return 0;
}
