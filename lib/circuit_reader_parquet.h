#ifndef NRN_READER_PARQUET_H
#define NRN_READER_PARQUET_H

#include "generic_reader.h"
#include "circuit_defs.h"
#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>


namespace neuron_parquet {
namespace circuit {


class CircuitReaderParquet : public Reader<CircuitData>
{
public:
    CircuitReaderParquet(const std::string & filename);
    ~CircuitReaderParquet();

    virtual uint32_t fillBuffer(CircuitData* buf, uint length) override;

    uint64_t record_count() const {
        return record_count_;
    }

    virtual uint32_t block_count() const override {
        return rowgroup_count_;
    }

    virtual inline void seek(uint64_t pos) override {
        cur_row_group_ = pos;
    }

    bool is_chunked() const {
        return true;
    }

private:


    // Variables
    std::unique_ptr<parquet::ParquetFileReader> reader_;
    std::shared_ptr<parquet::FileMetaData> parquet_metadata_;
    parquet::arrow::FileReader data_reader_;

    const uint32_t column_count_;
    const uint32_t rowgroup_count_;
    const uint64_t record_count_;
    uint32_t cur_row_group_;

};

}}  //ns nrn_parquet::circuit

#endif // NRN_READER_PARQUET_H
