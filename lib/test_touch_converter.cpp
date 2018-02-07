#include "touch_defs.h"
#include "touch_reader.h"
#include "touch_writer_parquet.h"
#include "converter.h"
#include "progress.h"


/// By using default buffers it is set
///  - 256K entries for converter (10MB)
///  - 64K entries for writer (2.5MB - it's transposing, so should fit in cache)
///  - 512K entries per parquet row_group

void convert_touches()  {
    TouchReader reader("file.touches") ;
    TouchWriterParquet writer("output.parquet");

    Converter<Touch> converter( reader, writer );

    ProgressMonitor p;
    converter.setProgressHandler(p.getNewHandler());

    converter.exportAll();

}

int main() {
    convert_touches();
}
