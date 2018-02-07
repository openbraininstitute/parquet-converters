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
    static const uint BUFFER_LEN = 512*1024;
    /// We transposing blocks of 600kB
    static const uint TRANSPOSE_LEN = 16 * 1024;

private:

    void _newRowGroup();

    inline void _writeDataSet(Touch* data, uint length);

    inline void _transpose_buffer_part(Touch* data, uint offset, uint length);

    inline void _writeBuffer(uint length);

    // Variables
    shared_ptr<GroupNode> touchSchema;
    std::shared_ptr<FileClass> out_file;
    shared_ptr<parquet::ParquetFileWriter> file_writer;
    parquet::RowGroupWriter* rg_writer;

    // Column writers
    parquet::Int32Writer* int32_writer;
    parquet::FloatWriter* float_writer;

    // Buffer
    struct BUF_T{
        int pre_neuron_id[BUFFER_LEN];
        int post_neuron_id[BUFFER_LEN];
        int pre_section[BUFFER_LEN];
        int pre_segment[BUFFER_LEN];
        int post_section[BUFFER_LEN];
        int post_segment[BUFFER_LEN];
        float pre_offset[BUFFER_LEN];
        float post_offset[BUFFER_LEN];
        float distance_soma[BUFFER_LEN];
        int branch_order[BUFFER_LEN];
    };

    std::unique_ptr<BUF_T> _buffer;

    // Shortcuts
    int *pre_neuron_id, *post_neuron_id, *pre_section, *pre_segment, *post_section, *post_segment, *branch_order;
    float *pre_offset, *post_offset, *distance_soma;
};

#endif // PARQUETWRITER_H
