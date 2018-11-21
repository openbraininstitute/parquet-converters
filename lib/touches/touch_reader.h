/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#ifndef LIB_TOUCHES_TOUCH_READER_H_
#define LIB_TOUCHES_TOUCH_READER_H_

#include <fstream>
#include <istream>
#include <functional>
#include <memory>
#include <vector>
#include <numeric>
#include <mutex>
#include <cstring>

#include "../generic_reader.h"
#include "./touch_defs.h"

namespace neuron_parquet {
namespace touches {


class TouchReader : public Reader<IndexedTouch> {
 public:
    TouchReader(const char *filename,
                bool buffered = false);
    ~TouchReader();

    bool is_chunked() const override {
        return false;
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

    // bytes per record
    static constexpr uint32_t RECORD_SIZE = sizeof(Touch);

 private:
    void _readHeader(const char* filename);
    void _fillBuffer();
    void _load_into(IndexedTouch* buf, uint32_t length);

    // File
    std::ifstream touchFile_;
    uint64_t record_count_;
    uint64_t offset_;
    bool endian_swap_;
    bool buffered_;

    // Internal Buffer: to be used for iteration
    uint32_t it_buf_index_;  // Offset relative to buffer
    uint32_t buffer_record_count_;
    std::unique_ptr<IndexedTouch[]> buffer_;
    std::unique_ptr<Touch[]> raw_buffer_;
};


}  // namespace touches
}  // namespace neuron_parquet

#endif  // LIB_TOUCHES_TOUCH_READER_H_
