/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#include <time.h>

#include <range/v3/all.hpp>

#include "touch_reader.h"

#define ARCHITECTURE_IDENTIFIER 1.001

using namespace ranges;

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
    int id;
    uint32_t count;
    long long offset;
};

struct HeaderSerialized {
    double architectureIdentifier;
    long long numberOfNeurons;
    char version[16];
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
    record_count_ = length / record_size_;
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

    version_ = V1;
    record_size_ = sizeof(v1::Touch);
    version_string_ = header.version;
    try {
        std::vector<int> vs = version_string_ | view::split('.')
                                              | view::transform([](const std::string& s) { return std::stoi(s); });
        if ((vs.size() >= 1 and vs[0] >= 6) or
            (vs.size() >= 2 and vs[0] >= 5 and vs[1] >= 4)) {
            version_ = V3;
            record_size_ = sizeof(v3::Touch);
        } else if (
                (vs.size() >= 1 and vs[0] >= 5) or
                (vs.size() >= 2 and vs[0] >= 4 and vs[1] >= 99)) {
            version_ = V2;
            record_size_ = sizeof(v2::Touch);
        }
    } catch (std::invalid_argument& e) {
        // Earlier versions were hashes of git commits. Default to V1.
    }

    std::vector<NeuronInfoSerialized> neurons(n);
    indexFile.read((char*) neurons.data(), sizeof(NeuronInfoSerialized) * n);
    if (endian_swap_) {
        for (uint64_t i = 0; i < n; ++i) {
            bswap(&neurons[i].id);
            bswap(&neurons[i].count);
            bswap(&neurons[i].offset);
        }
    }

    auto mm = std::minmax_element(std::begin(neurons),
                                  std::end(neurons),
                                  [](const auto& n1, const auto& n2) {
                                      return n1.id < n2.id;
                                  });

    first_ = mm.first->id;
    shifts_.resize(mm.second->id - first_ + 1);
    for (const auto& n: neurons) {
        auto pos = n.id - first_;
        if (shifts_[pos] > 0 and n.offset == 0 and n.count == 0) {
            std::cout << "[WARNING] Skipping empty entry for neuron ID " << n.id << std::endl;
        } else {
            shifts_[pos] = n.offset / record_size_;
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
        touchFile_.seekg(offset_ * record_size_);
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
    if (version_ == V1) {
        _load_touches<v1::Touch>(buffer, length);
    } else if (version_ == V2) {
        _load_touches<v2::Touch>(buffer, length);
    } else {
        _load_touches<v3::Touch>(buffer, length);
    }
}

template<typename T>
void TouchReader::_load_touches(IndexedTouch* buffer, uint32_t length) {
    static std::unique_ptr<T[]> rbuf;
    static uint32_t size = 0;
    if (length > size) {
        size = length;
        rbuf.reset(new T[size]);
    }
    touchFile_.read((char*)rbuf.get(), length * record_size_);

    if( endian_swap_ ){
        T* curTouch = rbuf.get();
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

    for (uint64_t i = 0; i < length; ++i) {
        int64_t gid = rbuf[i].pre_synapse_ids[NEURON_ID];
        int64_t index = i + offset_ - shifts_[gid - first_];
        if (index >= 1 << 24) {
            std::ostringstream o;
            o << "gid " << gid << " has more than 2^24 touches, "
              << "can't assign unique synapse indices";
            throw std::runtime_error(o.str());
        }
        int64_t touch_id = (gid << 24) + index;
        buffer[i] = IndexedTouch(rbuf[i], touch_id);
    }

    offset_ += length;
}


}  // namespace touches
}  // namespace neuron_parquet

