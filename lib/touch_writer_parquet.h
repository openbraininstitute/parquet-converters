#ifndef PARQUETWRITER_H
#define PARQUETWRITER_H

#include <parquet/api/writer.h>
#include <arrow/io/file.h>
#include "generic_writer.h"
#include "touch_defs.h"


using parquet::schema::GroupNode;
using FileClass = ::arrow::io::FileOutputStream;
using namespace std;


// RowGroup shall be defined as a multiple of the block read
/// 512k rows makes 20 MB groups (40 bytes/row)
/// That makes two 1MB pages per column
/// Since we are reading blocks of 5MB there will be four writes before we change the row group
static const int ROWS_PER_ROW_GROUP = 512*1024;


class TouchWriterParquet : Writer<Touch>
{
public:
    TouchWriterParquet(const string);
    ~TouchWriterParquet();

    // Inherited
    // void write(Touch* data, int length);  // offset are directly added to data ptr

private:

    void _newRowGroup();
    void _writeInCurRowGroup(Touch* data, unsigned length);

    shared_ptr<GroupNode> touchSchema;
    std::shared_ptr<FileClass> out_file;
    shared_ptr<parquet::ParquetFileWriter> file_writer;
    parquet::RowGroupWriter* rg_writer;

    int rg_size, rg_offset;

    //const
    const int STRUCT_LEN;

    // Column writers and

    parquet::Int32Writer* int32_writer;
    parquet::FloatWriter* float_writer;

    int* pre_neuron_id;
    int* post_neuron_id;
    int *pre_section, *pre_segment, *post_section, *post_segment;
    float* pre_offset;
    float* post_offset;
    float* distance_soma;
    int* branch_order;
};

#endif // PARQUETWRITER_H
