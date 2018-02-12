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
        return _rowgroup_count;
    }

    virtual inline void seek(uint pos) override;

private:


    // Variables
    shared_ptr<parquet::FileMetaData> _parquet_metadata;
    unique_ptr<parquet::ParquetFileReader> _reader;
    parquet::arrow::FileReader data_reader;

    uint _column_count;
    uint _rowgroup_count;
    uint _cur_row_group;

};

}}  //ns nrn_parquet::circuit

#endif // NRN_READER_PARQUET_H
