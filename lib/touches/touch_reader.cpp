/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#include "touch_reader.h"
#include <time.h>

#define ARCHITECTURE_IDENTIFIER 1.001

/// Some GCCs fail to detect the byte swap
template<typename T, size_t n>
struct bswapI;

template<typename T>
struct bswapI<T, 4> {
    static void swap(T* b) {
        *b=__builtin_bswap32(*b);
    };
};

template<typename T>
struct bswapI<T, 8> {
    static void swap(T* b) {
        *b=__builtin_bswap64(*b);
    };
};

template<typename T>
static inline void bswap(T* b) {
  bswapI<T, sizeof(T)>::swap(b);
}

namespace neuron_parquet {
namespace touches {

using namespace std;


// TouchDetector index file definitions
struct NeuronInfoSerialized {
    int neuronID;
    uint32_t touchesCount;
    long long binaryOffset;
};

struct HeaderSerialized {
    double architectureIdentifier;
    long long numberOfNeurons;
    char gitVersion[16];
};

TouchReader::TouchReader(const char* filename, bool buffered)
    : offset_(0)
    , buffered_(buffered)
    , it_buf_index_(0)
    , buffer_record_count_(0)
    , buffer_(new IndexedTouch[buffered ? BUFFER_LEN : 1])
{
    _readHeader(filename);

    touchFile_.open(filename, ifstream::binary);
    touchFile_.seekg (0, ifstream::end);
    uint64_t length = touchFile_.tellg();
    record_count_ = length / RECORD_SIZE;
    touchFile_.seekg (0);
}

TouchReader::~TouchReader() {
    touchFile_.close();
}


void
TouchReader::_readHeader(const char* filename) {
    string indexFilename(filename);
    auto idx = indexFilename.rfind("Data");
    if (idx == string::npos)
        throw runtime_error(string("Cannot determine index for file ") + filename);
    indexFilename.replace(idx, 4, "");

    std::ifstream indexFile(indexFilename, ifstream::binary);
    HeaderSerialized header;
    indexFile.read((char*) &header, sizeof(header));
    endian_swap_ = !(header.architectureIdentifier == ARCHITECTURE_IDENTIFIER);

    uint64_t n = header.numberOfNeurons;
    if (endian_swap_)
        bswap(&n);

    std::unique_ptr<NeuronInfoSerialized[]> neurons(new NeuronInfoSerialized[n]);
    indexFile.read((char*) neurons.get(), sizeof(NeuronInfoSerialized) * n);
    for (uint64_t i = 0; i < n; ++i) {
        if (endian_swap_) {
            bswap(&neurons[i].neuronID);
            bswap(&neurons[i].touchesCount);
            bswap(&neurons[i].binaryOffset);
        }
    }
}

IndexedTouch & TouchReader::begin() {
    return getItem(0);
}


///
/// \brief Implements a simple iteration protocol.
///        It assumes the buffer is in the correct position
///        If the user wants to change iteration he must call begin() or seek()
///
IndexedTouch & TouchReader::getNext() {
    uint32_t next_position = it_buf_index_ + 1;

    // Throw NULL if we are at the end
    if( offset_ + next_position >= record_count_ ) {
        throw NULL;
    }

    if (!buffered_) {
        _load_into(buffer_.get(), 1);
        return buffer_[0];
    }
    if(next_position >= buffer_record_count_) {
        // Need change buffer offset?
        if(next_position >= BUFFER_LEN) {
            seek(offset_ + next_position);
        }
        _fillBuffer();
    }
    return buffer_[it_buf_index_++];
}

///
/// \brief Returns a Touch record from an arbitrary index
///        It will reposition the internal iterator position, so getNext() will restart from the next item
///        Alternativelly use seek() to only define iterator position
///
IndexedTouch & TouchReader::getItem(uint32_t index) {
    seek(index);
    return getNext();
}


///
/// \brief TouchReader::seek Changes the file and buffers handlers
///        So that the specified position is contained in the buffer
///        NOTE: This functions doesnt imply any buffer filling.
/// \param pos
///
void TouchReader::seek(uint64_t pos)   {
    if( pos >= record_count_ ) {
        throw runtime_error(string("Invalid file position ") + to_string(pos));
    }

    uint64_t new_offset = pos;
    if (buffered_) {
        uint64_t nth_buffer = (pos / BUFFER_LEN);
        new_offset = nth_buffer * BUFFER_LEN;
    }

    if( new_offset != offset_ ) {
        offset_ = new_offset;
        touchFile_.seekg(offset_ * RECORD_SIZE);
        buffer_record_count_ = 0;
    }
    it_buf_index_ = pos - new_offset;
}


/**
 * @brief TouchReader::fillBuffer Fills a given buffer, incrementing the offset
 *        NOTE: This invalidates the internal buffer. A seek shall be performed before changing buffers
 * @return The number of records read
 */
uint32_t TouchReader::fillBuffer( IndexedTouch* buf, uint32_t load_n ) {
    if( load_n + offset_ > record_count_ ) {
        load_n = record_count_ - offset_;
    }

    _load_into( buf, load_n );
    return load_n;
}


void TouchReader::_fillBuffer() {
    uint32_t load_n = BUFFER_LEN;
    if( load_n + offset_ > record_count_ ) {
        load_n = record_count_ - offset_;
    }

    _load_into(buffer_.get(), load_n);
    buffer_record_count_ = load_n;
}


void TouchReader::_load_into(IndexedTouch* buffer, uint32_t length) {
    static std::unique_ptr<Touch[]> rbuf;
    static uint32_t size = 0;
    if (length > size) {
        size = length;
        rbuf.reset(new Touch[size]);
    }
    touchFile_.read((char*)rbuf.get(), length * RECORD_SIZE);

    if( endian_swap_ ){
        Touch* curTouch = rbuf.get();
        for(uint32_t i=0; i<length; i++) {
            // Given all the fields are contiguous and are 32bits long
            // we loop over them as if it was an array
            uint32_t* touch_data = (uint32_t*) (curTouch);
            for(int j=0; j<10; j++) {
                bswap(touch_data+j);
            }
            // next
            curTouch++;
        }
    }

    for (uint32_t i = 0; i < length; ++i) {
        buffer[i] = rbuf[i];
        buffer[i].synapse_index = i + offset_;
    }

    offset_ += length;
}


}  // namespace touches
}  // namespace neuron_parquet

