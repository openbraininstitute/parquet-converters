#include "touch_defs.h"
#include "touch_reader.h"
#include "touch_writer_parquet.h"
#include "converter.h"
#include "progress.h"

using namespace neuron_parquet;

/// By using default buffers it is set
///  - 512K entries for converter (loader) (20MB)
///  - 1K entries Transpose buffer
///  - 512K entries per parquet row_group

void convert_touches(char* filename)  {
    TouchReader reader(filename) ;
    TouchWriterParquet writer("output.parquet");

    Converter<Touch> converter( reader, writer );

    ProgressMonitor p(converter.n_blocks());
    converter.setProgressHandler(p.getNewHandler());

    p.task_start();
    converter.exportAll();

    p.task_done();
    std::cout << "\nDone." << std::endl;
}

int main(int argc, char* argv[]) {
    (void)argc;
    convert_touches(argv[1]);
    return 0;
}
