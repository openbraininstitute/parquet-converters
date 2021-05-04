/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#ifndef LIB_CONVERTER_H_
#define LIB_CONVERTER_H_

#include <iostream>
#include <functional>

#include <progress.hpp>
#include "./generic_reader.h"
#include "./generic_writer.h"

namespace neuron_parquet {

using ::utils::ProgressMonitor;


/**
 * \brief class Converter. Converts data between formats by reading blocks from a given
 * Reader and writing them using the given Writer.
 */
template<typename T>
class Converter {
 public:
    /**
     *  @brief The Format enum
     *  RECORDS: data is passed (between reader and writer) in a buffer (array) of the data type
     *  CHUNKS: data is passed as entire blocks defined by DataType, which shall internally manage the buffer*/
    enum class ConverterFormat {RECORDS, CHUNKS};

    // Default: 128K entries (~5MB)
    static const uint32_t DEFAULT_BUFFER_LEN = 128*1024;

    Converter(Reader<T> & reader,
              Writer<T> & writer,
              uint32_t buffer_len = DEFAULT_BUFFER_LEN)
        : BUFFER_LEN(reader.is_chunked()? 1 : buffer_len)
        , reader_(reader)
        , writer_(writer)
        , n_records_(reader_.record_count())
        , progress_handler_([](){})
    {
        if (reader_.is_chunked()) {
            // Buffer is a single chunk
            buffer_ = new T[1];
        } else {
            // Create a buffer of records.
            buffer_ = new T[(n_records_ > BUFFER_LEN)? BUFFER_LEN : n_records_];
        }
        writer_.setup(reader_.schema(), reader_.metadata());
    }


    ~Converter() {
        delete[] buffer_;
    }

    /**
     * @brief exportN: Conversion limited to a number of records
     * @param n
     * @param offset
     * @return
     */
    int exportN(uint64_t n, uint64_t offset = 0) {
        if (n == 0) {
            return n;
        }

        if (n + offset > n_records_) {
            n = n_records_ - offset;
            std::cerr << "Warning: Requested export blocks more than available. "
                         "Setting to max available."
                      << std::endl;
        }
        reader_.seek(offset);

        int n_buffers = n / BUFFER_LEN;
        int remaining = n % BUFFER_LEN;

        for (int i = 0; i < n_buffers; i++) {
            reader_.fillBuffer(buffer_, BUFFER_LEN);
            writer_.write(buffer_, BUFFER_LEN);
            progress_handler_();
        }
        if (remaining > 0) {
            reader_.fillBuffer(buffer_, remaining);
            writer_.write(buffer_, remaining);
            progress_handler_();
        }
        return n;
    }


    int exportAll() {
        int size = reader_.record_count();
        if (size == 0) {
            return size;
        }

        reader_.seek(0);
        uint32_t n;

        while ((n = reader_.fillBuffer(buffer_, BUFFER_LEN)) > 0) {
            writer_.write(buffer_, n);
            progress_handler_();
        }
        return size;
    }


    // T shall have += operator overloaded
    template<class P>
    void setProgressHandler(P& progress, int factor = 1) {
        progress_handler_ = [&progress, factor]() {
            progress += factor;
        };
    }

    /**
     *  \brief number_of_buffers Calculates the number of record buffers from the filesize, buffer len and record type
     *         NOTE: This function only makes sense for record buffers, not data chunks
     */
    static inline uint32_t number_of_buffers(uint64_t filesize,
                                             uint32_t bufferlen = DEFAULT_BUFFER_LEN) {
        uint32_t buffer_size = bufferlen * sizeof(T);
        return filesize / buffer_size + (filesize % buffer_size > 0);
    }

    const uint32_t BUFFER_LEN;

 private:
    ConverterFormat mode_;
    Reader<T>& reader_;
    Writer<T>& writer_;
    T* buffer_;

    const uint64_t n_records_;
    std::function<void()> progress_handler_;
};



}  // namespace neuron_parquet

#endif  // LIB_CONVERTER_H_
