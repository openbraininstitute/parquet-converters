#ifndef NRN_READER_PARQUET_H
#define NRN_READER_PARQUET_H

#include "generic_reader.h"
#include "circuit_defs.h"
#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>


namespace neuron_parquet {
namespace circuit {

using namespace std;

class CircuitReaderParquet : public Reader<CircuitData>
{
public:
    CircuitReaderParquet(const string & filename);
    ~CircuitReaderParquet();

    virtual uint fillBuffer(CircuitData* buf, uint length) override;

    virtual uint block_count() override {
        return rowgroup_count_;
    }

    virtual inline void seek(uint pos) override;

    size_t record_count() const {
        return record_count_;
    }

private:


    // Variables
    unique_ptr<parquet::ParquetFileReader> reader_;
    shared_ptr<parquet::FileMetaData> parquet_metadata_;
    parquet::arrow::FileReader data_reader_;

    const int column_count_;
    const int rowgroup_count_;
    const size_t record_count_;
    uint cur_row_group_;

};

}}  //ns nrn_parquet::circuit

#endif // NRN_READER_PARQUET_H
