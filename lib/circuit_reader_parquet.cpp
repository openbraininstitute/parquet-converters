#include "circuit_reader_parquet.h"
#include <stdexcept>

namespace neuron_parquet {
namespace circuit {

using std::string;
using std::vector;
using std::shared_ptr;

CircuitReaderParquet::CircuitReaderParquet(const string & filename)
  :
    filename_(filename),
    reader_(parquet::ParquetFileReader::OpenFile(filename, false)),
    parquet_metadata_(reader_->metadata()),
    column_count_(parquet_metadata_->num_columns()),
    rowgroup_count_(parquet_metadata_->num_row_groups()),
    record_count_(parquet_metadata_->num_rows())
{
}

void CircuitReaderParquet::close() {
    if(data_reader_) {
        data_reader_.reset(nullptr);
    }
    if(reader_) {
        reader_.reset(nullptr);
    }
}


void CircuitReaderParquet::init_data_reader() {
    if(! reader_) {
        reader_ = parquet::ParquetFileReader::OpenFile(filename_, false);
    }
    // NOTE that reader is unique. We must give it up to the other reader
    data_reader_.reset(new parquet::arrow::FileReader(arrow::default_memory_pool(), std::move(reader_)));
    cur_row_group_ = 0;
}


uint32_t CircuitReaderParquet::fillBuffer(CircuitData* buf, uint32_t length) {
    // We are using parquet::arrow::reader to recreate the data
    // parquet::reader is a low-level reader not handling automatically repetition levels, etc...
    (void) length;  // length is not used since the client always gets the full block

    if(!data_reader_) {
        init_data_reader();
    }
    if( cur_row_group_ >= rowgroup_count_) {
        return 0;
    }
    data_reader_->ReadRowGroup(cur_row_group_++, &(buf->row_group));
    return (uint32_t) buf->row_group->num_rows();
}


///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////

CircuitMultiReaderParquet::CircuitMultiReaderParquet(const vector<string> & filenames)
 :
   rowgroup_count_(0),
   record_count_(0),
   cur_file_(0)
{
    circuit_readers_.reserve(filenames.size());
    rowgroup_offsets_.reserve(filenames.size());
    rowgroup_offsets_.push_back(0);

    for(auto name : filenames) {
        shared_ptr<CircuitReaderParquet> reader(new CircuitReaderParquet(name));
        circuit_readers_.push_back(reader);
        rowgroup_count_ += reader->rowgroup_count_;
        record_count_ += reader->record_count_;
        // Offsets
        rowgroup_offsets_.push_back(rowgroup_count_);

        //Free file handler
        reader->close();
    }


}

uint32_t CircuitMultiReaderParquet::fillBuffer(CircuitData *buf, uint length) {
    uint32_t n = circuit_readers_[cur_file_]->fillBuffer(buf, length);
    if(n <= 0) {
        circuit_readers_[cur_file_]->close();
        cur_file_++;
        if(cur_file_ >= circuit_readers_.size()) {
            // EOF
            return 0;
        }
        n = circuit_readers_[cur_file_]->fillBuffer(buf, length);
    }
    return n;
}


void CircuitMultiReaderParquet::seek(uint64_t pos) {
    if( pos >= rowgroup_count_ ){
        throw std::runtime_error("Cant seek over file length");
    }

    cur_file_ = 0;
    for(unsigned int i=0; i<rowgroup_offsets_.size() && rowgroup_offsets_[i]<pos  ; i++) {
        cur_file_ = i;
    }

    uint32_t offset = pos - rowgroup_offsets_[cur_file_];
    circuit_readers_[cur_file_]->seek(offset);
}



}} // ns nrn_parquet::circuit
