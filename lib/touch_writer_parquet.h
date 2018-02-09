#ifndef PARQUETWRITER_H
#define PARQUETWRITER_H

#include <parquet/api/writer.h>
#include <arrow/io/file.h>
#include "generic_writer.h"
#include "touch_defs.h"


using parquet::schema::GroupNode;
using ParquetFileOutput = ::arrow::io::FileOutputStream;
using namespace std;


class TouchWriterParquet : public Writer<Touch>
{
public:
    TouchWriterParquet(const string);
    ~TouchWriterParquet();

    virtual void write(Touch* data, uint length) override;  // offset are directly added to data ptr



private:

    void _newRowGroup();

    inline void _writeDataSet(Touch* data, uint length);

    inline void _transpose_buffer_part(Touch* data, uint offset, uint length);

    inline void _writeBuffer(uint length);

    // Variables
    shared_ptr<GroupNode> touchSchema;
    std::shared_ptr<ParquetFileOutput> out_file;
    shared_ptr<parquet::ParquetFileWriter> file_writer;

    // Column writers
    parquet::Int32Writer* int32_writer;
    parquet::FloatWriter* float_writer;

    // Buffers
    // RowGroup shall be defined as a multiple of the block read
    /// 512k rows makes 20 MB groups (40 bytes/row)
    /// That makes two 1MB pages per column
    static const uint BUFFER_LEN = 512*1024;
    /// We transpose in small blocks for cache efficiency
    static const uint TRANSPOSE_LEN = 1024;

    uint _buffer_offset;
    
    template <int buf_len>
    struct BUF_T{
        int pre_neuron_id[buf_len];
        int post_neuron_id[buf_len];
        int pre_section[buf_len];
        int pre_segment[buf_len];
        int post_section[buf_len];
        int post_segment[buf_len];
        float pre_offset[buf_len];
        float post_offset[buf_len];
        float distance_soma[buf_len];
        int branch_order[buf_len];
    };

    std::unique_ptr<BUF_T<BUFFER_LEN>> _buffer;
    std::unique_ptr<BUF_T<TRANSPOSE_LEN>> _tbuffer;

};

#endif // PARQUETWRITER_H
