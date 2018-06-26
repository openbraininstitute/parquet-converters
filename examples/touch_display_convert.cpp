#include <iostream>
#include "touch_reader.h"
#include "parquet_reader.h"
#include "converter.h"
#include "progress.h"

using namespace std;

#define TEST_N_ENTRIES 20

int main(int argc, char *argv[])
{
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
    
    TouchReader reader(argv[1], swap_endians);

    printf( "| NRN_SRC | NRN_DST | SECTION/SEGMENT/OFFSET[SRC->DST] | DIST SOMA | BRANCH |\n" );
    printf( "|---------------------------------------------------------------------------------------------|\n" );
        for( int i=0; i<TEST_N_ENTRIES; i++) {
        Touch &t = reader.getNext();
        printf("| %7u | %7u | [%2u/%2u/%6.2f] -> [%2u/%2u/%6.2f] | %9.2f | %6d |\n",
                t.pre_synapse_ids[NEURON_ID],
                t.post_synapse_ids[NEURON_ID],
                t.pre_synapse_ids[SECTION_ID],
                t.pre_synapse_ids[SEGMENT_ID],
                t.pre_offset,
                t.post_synapse_ids[SECTION_ID],
                t.post_synapse_ids[SEGMENT_ID],
                t.post_offset,
                t.distance_soma, 
                t.branch
        );

    }

    string parquetFilename( argv[1] );
    std::size_t slashPos = parquetFilename.find_last_of("/\\");
    parquetFilename = parquetFilename.substr(slashPos+1);
    parquetFilename += ".parquet";
 
    TouchWriterParquet writer(parquetFilename);

    Converter<Touch> converter(reader, writer);
    ProgressMonitor p;
    converter.setProgressHandler(p.getNewHandler());
    
    converter.exportN( TEST_N_ENTRIES );
    printf("Done exporting\n");
    
    return 0;
}
