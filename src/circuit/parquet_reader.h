/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#pragma once

#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>
#include <string>
#include <vector>
#include "../generic_reader.h"
#include "./circuit_defs.h"

namespace neuron_parquet {
namespace circuit {


class CircuitReaderParquet : public Reader<CircuitData> {
    friend class CircuitMultiReaderParquet;

 public:
    explicit CircuitReaderParquet(const std::string & filename);

    ~CircuitReaderParquet() {}

    bool is_chunked() const override {
        return true;
    }

    uint64_t record_count() const override {
        return record_count_;
    }

    uint32_t block_count() const override {
        return rowgroup_count_;
    }

    void seek(uint64_t pos) override {
        cur_row_group_ = pos;
    }

    uint32_t fillBuffer(CircuitData* buf, uint length) override;

    virtual const CircuitData::Schema* schema() const override {
        return parquet_metadata_->schema();
    }

    virtual const std::shared_ptr<const CircuitData::Metadata> metadata() const override {
        return parquet_metadata_->key_value_metadata();
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
};



/**
 * @brief The CircuitMultiReaderParquet class
 *        Implements a reader of multiple combined files
 */
class CircuitMultiReaderParquet : public Reader<CircuitData> {
 public:
    explicit CircuitMultiReaderParquet(const std::vector<std::string> & filenames, const std::string& metadata_filename = "");

    ~CircuitMultiReaderParquet() {}

    bool is_chunked() const override {
        return true;
    }

    uint64_t record_count() const override {
        return record_count_;
    }

    uint32_t block_count() const override {
        return rowgroup_count_;
    }

    void seek(uint64_t pos) override;

    uint32_t fillBuffer(CircuitData* buf, uint length) override;

    virtual const CircuitData::Schema* schema() const override;

    virtual const std::shared_ptr<const CircuitData::Metadata> metadata() const override;
 private:
    std::vector<std::shared_ptr<CircuitReaderParquet>> circuit_readers_;
    std::shared_ptr<CircuitReaderParquet> metadata_reader_;
    uint32_t rowgroup_count_;
    uint64_t record_count_;
    std::vector<uint32_t> rowgroup_offsets_;
    uint32_t cur_row_group_;
    unsigned int cur_file_;
};



}  // namespace circuit
}  // namespace neuron_parquet
