#include "touch_defs.h"
#include "touch_reader.h"
#include "touch_writer_parquet.h"
#include "converter.h"
#include "progress.h"


static const unsigned BUFFER_LEN = 512*1024;

void convert_touches()  {
    TouchReader reader("file.touches") ;
    TouchWriterParquet writer("output.parquet");
    Converter <Touch, BUFFER_LEN> converter( reader, writer );

    ProgressHandler p;
    converter.setProgressHandler(p);

    converter.exportAll();

}
