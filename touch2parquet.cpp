#include <stdio.h>
#include "loader.h"
#include "parquetwriter.h"

typedef unsigned int uint;

int main( int argc, char* argv[] ) {

    if( argc != 2) {
        printf("usage: %s <touch_files ...>\n", argv[0]);
        return 1;
    }

    #pragma omp parallel for
    for( int i=1; i<argc; i++) {
        Loader tl(argv[i]);
        string parquetFilename( argv[i] );
        parquetFilename += ".parquet";
        ParquetWriter tw(parquetFilename.c_str());
        tl.setExporter( tw );
        tl.exportAll();

    }

    printf("Done exporting\n");

    return 0;
}
