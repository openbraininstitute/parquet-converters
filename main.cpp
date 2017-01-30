#include <iostream>
#include "loader.h"
#include "parquetwriter.h"

using namespace std;

int main(int argc, char *argv[])
{
    Loader tl("../touchesData.0");
    ParquetWriter tw("../touches.parquet");

    for( int i=0; i<20; i++) {
        Touch &t = tl.getNext();
        printf( "Touch Neurons %d - %d\n", t.getPreNeuronID(), t.getPostNeuronID() );
        printf( "  | Info: [%d-%d-%f] -> [%d-%d-%f]\n",
                t.pre_synapse_ids[SECTION_ID],
                t.pre_synapse_ids[SEGMENT_ID],
                t.pre_offset,
                t.post_synapse_ids[SECTION_ID],
                t.post_synapse_ids[SEGMENT_ID],
                t.post_offset
        );
        printf("  | Distance to soma: %f. Branch: %d\n", t.distance_soma, t.branch);

    }

    tl.setExporter( tw );
    //tl.exportN( 50 );
    tl.exportAll();

    printf("Done exporting\n");
    return 0;
}
