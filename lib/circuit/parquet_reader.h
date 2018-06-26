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
    ~CircuitReaderParquet(){}

    virtual uint32_t fillBuffer(CircuitData* buf, uint length) override;

    uint64_t record_count() const {
        return record_count_;
    }

    virtual uint32_t block_count() const override {
        return rowgroup_count_;
    }

    virtual void seek(uint64_t pos) override {
        cur_row_group_ = pos;
    }

    bool is_chunked() const {
        return true;
    }

private:
    const std::string filename_;
    std::unique_ptr<parquet::ParquetFileReader> reader_;
    std::shared_ptr<parquet::FileMetaData> parquet_metadata_;
    std::unique_ptr<parquet::arrow::FileReader> data_reader_;

    const uint32_t column_count_;
    const uint32_t rowgroup_count_;
    const uint64_t record_count_;
    uint32_t cur_row_group_;

    // Functions which might eventually be classed by friend class CircuitMultiReader
    /// Closes the underlying file handler.
    void close();
    /// Initializes the data reader
    void init_data_reader();

    friend class CircuitMultiReaderParquet;
};



/**
 * @brief The CircuitMultiReaderParquet class
 *        Implements a reader of multiple combined files
 */
class CircuitMultiReaderParquet : public Reader<CircuitData>
{
public:
    CircuitMultiReaderParquet(const std::vector<std::string> & filenames);
    ~CircuitMultiReaderParquet(){}

    virtual uint32_t fillBuffer(CircuitData* buf, uint length) override;

    uint64_t record_count() const {
        return record_count_;
    }

    virtual uint32_t block_count() const override {
        return rowgroup_count_;
    }

    virtual void seek(uint64_t pos) override;

    bool is_chunked() const {
        return true;
    }

private:
    std::vector<std::shared_ptr<CircuitReaderParquet>> circuit_readers_;
    uint32_t rowgroup_count_;
    uint64_t record_count_;    
    std::vector<uint32_t> rowgroup_offsets_;
    uint32_t cur_row_group_;
    unsigned int cur_file_;

};



}}  //ns nrn_parquet::circuit

#endif // NRN_READER_PARQUET_H
