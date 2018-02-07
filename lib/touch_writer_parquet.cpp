#include "touch_writer_parquet.h"

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


TouchWriterParquet::TouchWriterParquet(const string filename):
  rg_freeSlots(0),
  buffer_freeSlots(0)
{
    // Create a ParquetFileWriter instance
    PARQUET_THROW_NOT_OK(FileClass::Open(filename.c_str(), &out_file));
    touchSchema = setupSchema();

    WriterProperties::Builder prop_builder;
    prop_builder.disable_dictionary();
    prop_builder.compression(Compression::SNAPPY);
    file_writer = ParquetFileWriter::Open(out_file, touchSchema, prop_builder.build());

    // Allocate contiguou buffer and get direct pointers
    _buffer.reset(new BUF_T());
    pre_neuron_id = _buffer->pre_neuron_id;
    post_neuron_id = _buffer->post_neuron_id;
    pre_section = _buffer->pre_section;
    pre_segment = _buffer->pre_segment;
    post_section = _buffer->post_section;
    post_segment = _buffer->post_segment;
    pre_offset = _buffer->pre_offset;
    post_offset = _buffer->post_offset;
    distance_soma = _buffer->distance_soma;
    branch_order = _buffer->branch_order;
}


TouchWriterParquet::~TouchWriterParquet() {
    file_writer->Close();
    out_file->Close();
}


void TouchWriterParquet::_newRowGroup() {
    rg_writer = file_writer->AppendRowGroup();
    rg_freeSlots = ROWS_PER_ROW_GROUP;
}


void TouchWriterParquet::write(Touch* data, uint length) {

    while( length > 0 ) {
        //Allocate new Row group if limit reached
        if( rg_freeSlots == 0 ) {
            _newRowGroup();
        }

        uint n_write = length;
        if( rg_freeSlots < n_write ) {
            n_write = rg_freeSlots;
        }

        //Write to the current row buffer
        _writeChunkedCurRowGroup( data, n_write );
        data   += n_write;
        length -= n_write;
    }
}


// We need to chunk to avoid large buffers not fitting in cache
void TouchWriterParquet::_writeChunkedCurRowGroup(Touch* data, uint length) {
    uint n_chunks = length / BUFFER_LEN;
    uint remaining = length % BUFFER_LEN;

    for( uint i=0; i<n_chunks; i++) {
        _writeInCurRowGroup(data, BUFFER_LEN);
        data += BUFFER_LEN;
    }
    _writeInCurRowGroup(data, remaining);
}


///
/// Low-level function to write directly a Touch set to the currently open row group
///
void TouchWriterParquet::_writeInCurRowGroup(Touch* data, uint length) {


    for( uint i=0; i< length; i++  ) {
        pre_neuron_id[i] = data[i].getPreNeuronID();
        post_neuron_id[i] = data[i].getPostNeuronID();
        pre_offset[i] = data[i].pre_offset;
        post_offset[i] = data[i].post_offset;
        distance_soma[i] = data[i].distance_soma;
        branch_order[i] = data[i].branch;
        pre_section[i] = data[i].pre_synapse_ids[SECTION_ID];
        pre_segment[i] = data[i].pre_synapse_ids[SEGMENT_ID];
        post_section[i] = data[i].post_synapse_ids[SECTION_ID];
        post_segment[i] = data[i].post_synapse_ids[SEGMENT_ID];
        if( pre_section[i]>0x7fff ) {
            printf("Problematic pre_section %d\n", pre_section[i]);
            throw "Invalid pre_section. Please check endianess";
        }
        if( _buffer->pre_segment[i]>0x7fff ) printf("Problematic pre_segment %d\n", pre_segment[i]);
        if( _buffer->post_section[i]>0x7fff ) printf("Problematic post_section %d\n", post_section[i]);
        if( _buffer->post_segment[i]>0x7fff ) printf("Problematic post_segment %d\n", post_segment[i]);
    }

    //pre_neuron / post_neuron [ids, section, segment]
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, pre_neuron_id);
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, post_neuron_id);
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, pre_section);
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, pre_segment);
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, post_section);
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, post_segment);

    //pre_offset
    float_writer = static_cast<FloatWriter*>(rg_writer->NextColumn());
    float_writer->WriteBatch(length, nullptr, nullptr, pre_offset);

    //post_offset
    float_writer = static_cast<FloatWriter*>(rg_writer->NextColumn());
    float_writer->WriteBatch(length, nullptr, nullptr, post_offset);

    //distance_soma
    float_writer = static_cast<FloatWriter*>(rg_writer->NextColumn());
    float_writer->WriteBatch(length, nullptr, nullptr, distance_soma);

    //branch_order
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, branch_order);

    rg_freeSlots -= length;
}
