#include "circuit_reader_parquet.h"


namespace neuron_parquet {
namespace circuit {

CircuitReaderParquet::CircuitReaderParquet(const string & filename)
  : _reader(parquet::ParquetFileReader::OpenFile(filename, false)),
    _parquet_metadata(_reader->metadata()),
    _column_count(_parquet_metadata->num_columns()),
    _rowgroup_count(_parquet_metadata->num_row_groups()),
    data_reader(arrow::default_memory_pool(), _reader)
{
    seek(0);
}

CircuitReaderParquet::~CircuitReaderParquet(){
    parquet_reader->close();
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
    data_reader.ReadRowGroup(_cur_row_group++, &(buf->row_group));
    return (uint) buf->row_group->num_rows;
}


void CircuitReaderParquet::seek(uint pos) {
    _cur_row_group = pos;
}


}} // ns nrn_parquet::circuit
