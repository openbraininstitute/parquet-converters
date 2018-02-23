#include "circuit_reader_parquet.h"


namespace neuron_parquet {
namespace circuit {

CircuitReaderParquet::CircuitReaderParquet(const string & filename)
  :
    reader_(parquet::ParquetFileReader::OpenFile(filename, false)),
    parquet_metadata_(reader_->metadata()),
    data_reader_(arrow::default_memory_pool(), std::move(reader_)),

    column_count_(parquet_metadata_->num_columns()),
    rowgroup_count_(parquet_metadata_->num_row_groups()),
    record_count_(parquet_metadata_->num_rows()),
    cur_row_group_(0)
{
}

CircuitReaderParquet::~CircuitReaderParquet(){

}


uint32_t CircuitReaderParquet::fillBuffer(CircuitData* buf, uint32_t length) {
    // We are using parquet::arrow::reader to recreate the data
    // parquet::reader is a low-level reader not handling automatically repetition levels, etc...

    data_reader_.ReadRowGroup(cur_row_group_++, &(buf->row_group));
    return (uint32_t) buf->row_group->num_rows();
}


}} // ns nrn_parquet::circuit
