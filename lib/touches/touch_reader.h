/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#ifndef LIB_TOUCHES_TOUCH_READER_H_
#define LIB_TOUCHES_TOUCH_READER_H_

#include <cstring>
#include <fstream>
#include <functional>
#include <istream>
#include <memory>
#include <mutex>
#include <numeric>
#include <vector>

#include "../generic_reader.h"
#include "./touch_defs.h"

namespace neuron_parquet {
namespace touches {


class TouchReader : public Reader<IndexedTouch> {
 public:
    TouchReader(const char *filename,
                bool buffered = false);
    ~TouchReader();

    Version version() const { return version_; }
    std::string version_string() const { return version_string_; }

    bool is_chunked() const override {
        return false;
    }

    uint32_t record_size() const {
        return record_size_;
    }

    uint64_t record_count() const override {
        return record_count_;
    }

    uint32_t block_count() const override {
        return record_count_ / BUFFER_LEN + (record_count_ % BUFFER_LEN > 0);
    }

    void seek(uint64_t pos) override;

    uint32_t fillBuffer(IndexedTouch* buf, uint32_t length) override;

    // Iteration
    IndexedTouch & begin();
    IndexedTouch & end();
    IndexedTouch & getNext();
    IndexedTouch & getItem(uint32_t index);

    static const uint32_t BUFFER_LEN = 256;

    virtual const void* schema() const override { return nullptr; };
    virtual const std::shared_ptr<const void> metadata() const override { return std::shared_ptr<const void>(); };

 private:
    void _readHeader(const char* filename);
    void _fillBuffer();

    void _load_into(IndexedTouch* buffer, uint32_t length);

    template<typename T>
    void _load_touches(IndexedTouch* buffer, uint32_t length);

    // File
    std::ifstream touchFile_;
    uint32_t record_size_;
    uint64_t record_count_;
    uint64_t offset_;
    bool endian_swap_;
    bool buffered_;
    Version version_;
    std::string version_string_;

    // Internal Buffer: to be used for iteration
    uint32_t it_buf_index_;  // Offset relative to buffer
    uint32_t buffer_record_count_;
    std::unique_ptr<IndexedTouch[]> buffer_;

    // Store the offset that touches need to be shifted to construct the
    // unique synapse id.
    std::vector<int64_t> shifts_;
    std::size_t first_;
};


}  // namespace touches
}  // namespace neuron_parquet

#endif  // LIB_TOUCHES_TOUCH_READER_H_
