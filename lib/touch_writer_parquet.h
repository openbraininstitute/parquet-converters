#ifndef PARQUETWRITER_H
#define PARQUETWRITER_H

#include <parquet/api/writer.h>
#include <arrow/io/file.h>
#include "generic_writer.h"
#include "touch_defs.h"


using parquet::schema::GroupNode;
using FileClass = ::arrow::io::FileOutputStream;
using namespace std;


class TouchWriterParquet : public Writer<Touch>
{
public:
    TouchWriterParquet(const string);
    ~TouchWriterParquet();

    virtual void write(Touch* data, uint length) override;  // offset are directly added to data ptr


    // RowGroup shall be defined as a multiple of the block read
    /// 512k rows makes 20 MB groups (40 bytes/row)
    /// That makes two 1MB pages per column
    /// Since we are transposing blocks of 2.5MB (buffer_len) there will be 8 writes before we change the row group
    static const uint BUFFER_LEN = 64 * 1024;
    static const uint ROWS_PER_ROW_GROUP = 512*1024;

private:

    void _newRowGroup();
    inline void _writeChunkedCurRowGroup(Touch* data, uint length);
    inline void _writeInCurRowGroup(Touch* data, uint length);

    shared_ptr<GroupNode> touchSchema;
    std::shared_ptr<FileClass> out_file;
    shared_ptr<parquet::ParquetFileWriter> file_writer;
    parquet::RowGroupWriter* rg_writer;

    uint rg_freeSlots;
    uint buffer_freeSlots;

    // Column writers

    parquet::Int32Writer* int32_writer;
    parquet::FloatWriter* float_writer;

    char* _mem_block;
    int* pre_neuron_id;
    int* post_neuron_id;
    int *pre_section, *pre_segment, *post_section, *post_segment;
    float* pre_offset;
    float* post_offset;
    float* distance_soma;
    int* branch_order;
};

#endif // PARQUETWRITER_H
