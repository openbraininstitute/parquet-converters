/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#pragma once

#include <parquet/api/writer.h>
#include <arrow/io/file.h>
#include "../generic_writer.h"
#include "touch_defs.h"

namespace neuron_parquet {
namespace touches {

using parquet::schema::GroupNode;
using ParquetFileOutput = ::arrow::io::FileOutputStream;
using namespace std;


class TouchWriterParquet : public Writer<IndexedTouch>
{
public:
    TouchWriterParquet(const string, Version, const std::string&);
    ~TouchWriterParquet();

    virtual void write(const IndexedTouch* data, uint32_t length) override;  // offset are directly added to data ptr

    virtual void setup(const void*, std::shared_ptr<const void>) override {};


private:

    void _newRowGroup();

    inline void _writeDataSet(const IndexedTouch* data, uint length);

    inline void _transpose_buffer_part(const IndexedTouch* data, uint offset, uint length);

    inline void _writeBuffer(uint length);

    // Variables
    Version version;
    shared_ptr<GroupNode> touchSchema;
    std::shared_ptr<::arrow::io::FileOutputStream> out_file;
    shared_ptr<parquet::ParquetFileWriter> file_writer;

    // Column writers
    parquet::Int32Writer* int32_writer;
    parquet::Int64Writer* int64_writer;
    parquet::FloatWriter* float_writer;

    // Buffers
    // RowGroup shall be defined as a multiple of the block read
    /// 512k rows makes 20 MB groups (40 bytes/row)
    /// That makes two 1MB pages per column
    static const uint BUFFER_LEN = 512*1024;
    /// We transpose in small blocks for cache efficiency
    static const uint TRANSPOSE_LEN = 1024;

    // TouchDetector compresses the branch types into a single byte (0 =
    // soma). We need to unpack them by shifting & masking, and then
    // introduce an offset to match the MorphIO convention (0 = invalid,
    // 1 = soma, …)
    static const std::size_t BRANCH_MASK = 0xF;
    static const std::size_t BRANCH_SHIFT = 4;
    static const std::size_t BRANCH_OFFSET = 1;

    uint _buffer_offset;

    template <int buf_len>
    struct BUF_T{
        long long synapse_id[buf_len];
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
        float pre_section_fraction[buf_len];
        float post_section_fraction[buf_len];
        float pre_position[3][buf_len];
        float post_position[3][buf_len];
        float spine_length[buf_len];
        int pre_branch_type[buf_len];
        int post_branch_type[buf_len];
        float pre_position_center[3][buf_len];
        float post_position_surface[3][buf_len];
    };

    std::unique_ptr<BUF_T<BUFFER_LEN>> _buffer;
    std::unique_ptr<BUF_T<TRANSPOSE_LEN>> _tbuffer;
};


}  // namespace touches
}  // namespace neuron_parquet
