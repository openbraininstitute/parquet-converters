#include <iostream>
#include "loader.h"
#include "parquetwriter.h"

using namespace std;

int main(int argc, char *argv[])
{
    Loader tl(argv[1], true);
    ParquetWriter tw("touches__.parquet");

    for( int i=0; i<20; i++) {
        Touch &t = tl.getNext();
        printf( "Touch Neurons %u - %u\n", t.getPreNeuronID(), t.getPostNeuronID() );
        printf( "  | Info: [%u-%u-%f] -> [%u-%u-%f]\n",
                t.pre_synapse_ids[SECTION_ID],
                t.pre_synapse_ids[SEGMENT_ID],
                t.pre_offset,
                t.post_synapse_ids[SECTION_ID],
                t.post_synapse_ids[SEGMENT_ID],
                t.post_offset
        );
        printf("  | Distance to soma: %.2f. Branch: %d\n", t.distance_soma, t.branch);

    }

    tl.setExporter( tw );
    tl.exportN( 20 );
    //tl.exportAll();

    printf("Done exporting\n");
    return 0;
}
