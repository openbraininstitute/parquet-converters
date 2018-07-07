/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#include <neuron_parquet/touches.h>
#include <progress.hpp>


using namespace neuron_parquet;

/// By using default buffers it is set
///  - 512K entries for converter (loader) (20MB)
///  - 1K entries Transpose buffer
///  - 512K entries per parquet row_group

void convert_touches(char* filename)  {
    TouchReader reader(filename) ;
    TouchWriterParquet writer("output.parquet");

    {
        Converter<Touch> converter( reader, writer );
        ProgressMonitor p(reader.block_count());

        converter.setProgressHandler(p);

        converter.exportAll();
    }

    std::cout << "\nDone." << std::endl;
}

int main(int argc, char* argv[]) {
    (void)argc;
    convert_touches(argv[1]);
    return 0;
}
