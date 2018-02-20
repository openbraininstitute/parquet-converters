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


uint CircuitReaderParquet::fillBuffer(CircuitData* buf, uint length) {
    // We are using parquet::arrow::reader to recreate the data
    // parquet::reader is a low-level reader not handling automatically repetition levels, etc...

//    for(int i; i<_column_count; i++) {
//        char * buffer = new t;
//        int64_t total_rows_read=0, single_read_rows;
//        col_reader = row_group_reader->Column(i);
//        while (col_reader->HasNext()) {
//            col_reader->ReadBatch(num_rows, nullptr, nullptr, buffer, &single_read_rows);
//            buffer += single_read_rows;
//            total_rows_read += single_read_rows;
//        }
//    }
    data_reader_.ReadRowGroup(cur_row_group_++, &(buf->row_group));
    return (uint) buf->row_group->num_rows();
}


void CircuitReaderParquet::seek(uint pos) {
    cur_row_group_ = pos;
}


}} // ns nrn_parquet::circuit
