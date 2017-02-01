#ifndef PARQUETWRITER_H
#define PARQUETWRITER_H

#define COMBINE_SECTION_SEGMENT_FIELDS 0

#include <parquet/api/writer.h>
#include <arrow/io/file.h>
#include "loader.h"


using parquet::schema::GroupNode;
using FileClass = ::arrow::io::FileOutputStream;
using namespace std;

class ParquetWriter
{
public:
    ParquetWriter(const char*);
    ~ParquetWriter();
    void write(Touch* data, int length);
private:
    static const int NUM_ROWS_PER_ROW_GROUP = Loader::TOUCH_BUFFER_LEN;
    static const int MIN_ROWS_PER_GROUP = 16;

    void _newRowGroup(int n_rows=Loader::TOUCH_BUFFER_LEN);

    shared_ptr<GroupNode> touchSchema;
    std::shared_ptr<FileClass> out_file;
    shared_ptr<parquet::ParquetFileWriter> file_writer;
    parquet::RowGroupWriter* rg_writer;

    parquet::Int32Writer* int32_writer;
    parquet::FloatWriter* float_writer;

    int rg_size, rg_offset;

    //const
    const int STRUCT_LEN;

    int* pre_neuron_id;
    int* post_neuron_id;
#if COMBINE_SECTION_SEGMENT_FIELDS
    int* pre_post_sect_segm;
#else
    int *pre_section, *pre_segment, *post_section, *post_segment;
#endif
    float* pre_offset;
    float* post_offset;
    float* distance_soma;
    int* branch_order;
};

#endif // PARQUETWRITER_H
