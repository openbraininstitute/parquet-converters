/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#include "parquet_writer.h"
#include <assert.h>

using namespace parquet;


static std::shared_ptr<GroupNode> setupSchema() {
  schema::NodeVector fields;

  fields.push_back(schema::PrimitiveNode::Make(
      "pre_neuron_id", Repetition::REQUIRED, Type::INT32, LogicalType::INT_32));

  fields.push_back(schema::PrimitiveNode::Make(
      "post_neuron_id", Repetition::REQUIRED, Type::INT32, LogicalType::INT_32));

  // POSITION OF THE SYNAPSE //
  fields.push_back(schema:: PrimitiveNode::Make(
       "pre_section", Repetition::REQUIRED, Type::INT32, LogicalType::INT_16 ) );
  fields.push_back(schema:: PrimitiveNode::Make(
       "pre_segment", Repetition::REQUIRED, Type::INT32, LogicalType::INT_16 ) );
  fields.push_back(schema:: PrimitiveNode::Make(
       "post_section", Repetition::REQUIRED, Type::INT32, LogicalType::INT_16 ) );
  fields.push_back(schema:: PrimitiveNode::Make(
       "post_segment", Repetition::REQUIRED, Type::INT32, LogicalType::INT_16 ) );

  fields.push_back(schema::PrimitiveNode::Make(
      "pre_offset", Repetition::REQUIRED, Type::FLOAT, LogicalType::NONE));

  fields.push_back(schema::PrimitiveNode::Make(
      "post_offset", Repetition::REQUIRED, Type::FLOAT, LogicalType::NONE));

  fields.push_back(schema::PrimitiveNode::Make(
      "distance_soma", Repetition::REQUIRED, Type::FLOAT, LogicalType::NONE));

  fields.push_back(schema::PrimitiveNode::Make(
      "branch_order", Repetition::REQUIRED, Type::INT32, LogicalType::INT_8));

  // Create a GroupNode named 'schema' using the primitive nodes defined above
  // This GroupNode is the root node of the schema tree
  return std::static_pointer_cast<schema::GroupNode>(
      GroupNode::Make("touchSchema", Repetition::REQUIRED, fields));
}


TouchWriterParquet::TouchWriterParquet(const string filename)
 : _buffer_offset(0)
{
    // Create a ParquetFileWriter instance
    PARQUET_THROW_NOT_OK(ParquetFileOutput::Open(filename.c_str(), &out_file));
    touchSchema = setupSchema();

    WriterProperties::Builder prop_builder;
    prop_builder.compression(Compression::SNAPPY);
    prop_builder.disable_dictionary();
    
    file_writer = ParquetFileWriter::Open(out_file, touchSchema, prop_builder.build());

    // Allocate contiguous buffers for FULL output and TRANSPOSED block
    // Too big to put in stack
    _buffer.reset(new BUF_T<BUFFER_LEN>());
    _tbuffer.reset(new BUF_T<TRANSPOSE_LEN>());
}


TouchWriterParquet::~TouchWriterParquet() {
    if( _buffer_offset > 0 ) {
        // Flush remaining data
        _writeBuffer(_buffer_offset);
    }
    file_writer->Close();
    out_file->Close();
}


void TouchWriterParquet::write(const Touch* data, uint32_t length) {

    //Split large Data in BUFFER_SIZE chunks
    while( length > 0 ) {
        uint32_t write_n = BUFFER_LEN - _buffer_offset;
        if( length < write_n ) {
            write_n = length;
        }

        //Write sub dataset
        _writeDataSet( data, write_n );
        data   += write_n;
        length -= write_n;
    }
}


// We need to chunk to avoid large buffers not fitting in cache
void TouchWriterParquet::_writeDataSet(const Touch* data, uint length) {
    uint n_chunks = length / TRANSPOSE_LEN;
    uint remaining = length % TRANSPOSE_LEN;

    for( uint i=0; i<n_chunks; i++) {
        _transpose_buffer_part(data, i*TRANSPOSE_LEN, TRANSPOSE_LEN);
    }
    _transpose_buffer_part(data, n_chunks*TRANSPOSE_LEN, remaining);

    assert(_buffer_offset+length <= BUFFER_LEN);

    if( _buffer_offset+length == BUFFER_LEN ) {
        // We only write full buffers
        // Remaining buffer data only on destruction
        _writeBuffer(BUFFER_LEN);
        _buffer_offset = 0;
    }
    else {
        _buffer_offset+=length;
    }
}


void TouchWriterParquet::_transpose_buffer_part(const Touch* data, uint offset, uint length) {
    // Here we are transposing using the TBuffer
    // Indexes at 0, so we also advance the local ptr to the offset
    data += offset;

    for( uint i=0; i<length; i++ ) {
        _tbuffer->pre_neuron_id[i] = data[i].getPreNeuronID();
        _tbuffer->post_neuron_id[i] = data[i].getPostNeuronID();
        _tbuffer->pre_offset[i] = data[i].pre_offset;
        _tbuffer->post_offset[i] = data[i].post_offset;
        _tbuffer->distance_soma[i] = data[i].distance_soma;
        _tbuffer->branch_order[i] = data[i].branch;
        _tbuffer->pre_section[i] = data[i].pre_synapse_ids[SECTION_ID];
        _tbuffer->pre_segment[i] = data[i].pre_synapse_ids[SEGMENT_ID];
        _tbuffer->post_section[i] = data[i].post_synapse_ids[SECTION_ID];
        _tbuffer->post_segment[i] = data[i].post_synapse_ids[SEGMENT_ID];
        if( _tbuffer->pre_section[i]>0x7fff ) {
            printf("Problematic pre_section %d\n", _tbuffer->pre_section[i]);
            throw runtime_error("Invalid pre_section. Please check endianess");
        }
        if( _tbuffer->pre_segment[i]>0x7fff ) printf("Problematic pre_segment %d\n", _tbuffer->pre_segment[i]);
        if( _tbuffer->post_section[i]>0x7fff ) printf("Problematic post_section %d\n", _tbuffer->post_section[i]);
        if( _tbuffer->post_segment[i]>0x7fff ) printf("Problematic post_segment %d\n", _tbuffer->post_segment[i]);
    }

    // Append to main buffer
    uint buffer_offset = _buffer_offset + offset;
    std::copy( _tbuffer->pre_neuron_id,  _tbuffer->pre_neuron_id+length,  _buffer->pre_neuron_id+buffer_offset );
    std::copy( _tbuffer->post_neuron_id, _tbuffer->post_neuron_id+length, _buffer->post_neuron_id+buffer_offset );
    std::copy( _tbuffer->pre_offset,     _tbuffer->pre_offset+length,     _buffer->pre_offset+buffer_offset );
    std::copy( _tbuffer->post_offset,    _tbuffer->post_offset+length,    _buffer->post_offset+buffer_offset );
    std::copy( _tbuffer->distance_soma,  _tbuffer->distance_soma+length,  _buffer->distance_soma+buffer_offset );
    std::copy( _tbuffer->branch_order,   _tbuffer->branch_order+length,   _buffer->branch_order+buffer_offset );
    std::copy( _tbuffer->pre_section,    _tbuffer->pre_section+length,    _buffer->pre_section+buffer_offset );
    std::copy( _tbuffer->pre_segment,    _tbuffer->pre_segment+length,    _buffer->pre_segment+buffer_offset );
    std::copy( _tbuffer->post_section,   _tbuffer->post_section+length,   _buffer->post_section+buffer_offset );
    std::copy( _tbuffer->post_segment,   _tbuffer->post_segment+length,   _buffer->post_segment+buffer_offset );
}


///
/// Low-level function to write directly a Touch set to the currently open row group
///
void TouchWriterParquet::_writeBuffer(uint length) {
    RowGroupWriter* rg_writer = file_writer->AppendRowGroup();

    //pre_neuron / post_neuron [ids, section, segment]
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, _buffer->pre_neuron_id);
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, _buffer->post_neuron_id);
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, _buffer->pre_section);
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, _buffer->pre_segment);
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, _buffer->post_section);
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, _buffer->post_segment);

    //pre_offset
    float_writer = static_cast<FloatWriter*>(rg_writer->NextColumn());
    float_writer->WriteBatch(length, nullptr, nullptr, _buffer->pre_offset);

    //post_offset
    float_writer = static_cast<FloatWriter*>(rg_writer->NextColumn());
    float_writer->WriteBatch(length, nullptr, nullptr, _buffer->post_offset);

    //distance_soma
    float_writer = static_cast<FloatWriter*>(rg_writer->NextColumn());
    float_writer->WriteBatch(length, nullptr, nullptr, _buffer->distance_soma);

    //branch_order
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, _buffer->branch_order);

}
