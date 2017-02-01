#include "parquetwriter.h"

using namespace parquet;

static std::shared_ptr<GroupNode> setupSchema() {
  schema::NodeVector fields;

  fields.push_back(schema::PrimitiveNode::Make(
      "pre_neuron_id", Repetition::REQUIRED, Type::INT32, LogicalType::INT_32));

  fields.push_back(schema::PrimitiveNode::Make(
      "post_neuron_id", Repetition::REQUIRED, Type::INT32, LogicalType::INT_32));

  // POSITION OF THE SYNAPSE //
  #if COMBINE_SECTION_SEGMENT_FIELDS
  //Lets attempt to store all 4 fields in 32bits
  fields.push_back(schema:: PrimitiveNode::Make(
       "pre_post_sect_segm", Repetition::REQUIRED, Type::INT32, LogicalType::INT_32 ) );
  #else
  fields.push_back(schema:: PrimitiveNode::Make(
       "pre_section", Repetition::REQUIRED, Type::INT32, LogicalType::INT_16 ) );
  fields.push_back(schema:: PrimitiveNode::Make(
       "pre_segment", Repetition::REQUIRED, Type::INT32, LogicalType::INT_16 ) );
  fields.push_back(schema:: PrimitiveNode::Make(
       "post_section", Repetition::REQUIRED, Type::INT32, LogicalType::INT_16 ) );
  fields.push_back(schema:: PrimitiveNode::Make(
       "post_segment", Repetition::REQUIRED, Type::INT32, LogicalType::INT_16 ) );
  #endif

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


ParquetWriter::ParquetWriter(const char* filename):
  STRUCT_LEN( sizeof(Touch) ),
  pre_neuron_id( new int[NUM_ROWS_PER_ROW_GROUP] ),
  post_neuron_id( new int[NUM_ROWS_PER_ROW_GROUP] ),
#if COMBINE_SECTION_SEGMENT_FIELDS
  pre_post_sect_segm( new int[NUM_ROWS_PER_ROW_GROUP] ),
#else
  pre_section( new int[NUM_ROWS_PER_ROW_GROUP] ),
  pre_segment( new int[NUM_ROWS_PER_ROW_GROUP] ),
  post_section( new int[NUM_ROWS_PER_ROW_GROUP] ),
  post_segment( new int[NUM_ROWS_PER_ROW_GROUP] ),
#endif
  pre_offset( new float[NUM_ROWS_PER_ROW_GROUP] ),
  post_offset( new float[NUM_ROWS_PER_ROW_GROUP] ),
  distance_soma( new float[NUM_ROWS_PER_ROW_GROUP] ),
  branch_order( new int[NUM_ROWS_PER_ROW_GROUP] )
{
    // Create a ParquetFileWriter instance
    PARQUET_THROW_NOT_OK(FileClass::Open(filename, &out_file));
    touchSchema = setupSchema();

    WriterProperties::Builder prop_builder;
    prop_builder.compression(Compression::SNAPPY);
    prop_builder.disable_dictionary();
    file_writer = ParquetFileWriter::Open(out_file, touchSchema, prop_builder.build());
    //file_writer = parquet::ParquetFileWriter::Open(out_file, touchSchema);

    rg_size = 0;
    rg_offset = 0;
}

ParquetWriter::~ParquetWriter() {
    file_writer->Close();
    out_file->Close();
    delete pre_neuron_id;
    delete post_neuron_id;
    delete pre_offset;
    delete post_offset;
    delete distance_soma;
    delete branch_order;
#if COMBINE_SECTION_SEGMENT_FIELDS
    delete pre_post_sect_segm;
#else
    delete pre_section;
    delete pre_segment;
    delete post_section;
    //delete post_segment;
#endif
}


void ParquetWriter::_newRowGroup(int n_rows) {
    // Append a RowGroup with a specific number of rows.
    rg_writer = file_writer->AppendRowGroup(n_rows);
    rg_size = n_rows;
    rg_offset = 0;
}


void ParquetWriter::write(Touch* data, int length) {
    //Allocate new Row group if no space avail
    int freeSlots = rg_size - rg_offset;
    if( freeSlots < length ) { //freeslots actually should be 0
        if(freeSlots > 0) {
            //fill up the current row buffer
            write( data, freeSlots );
            data   += freeSlots;
            length -= freeSlots;
        }
        // Guaranteed 0 free slots now - need allocate
        if( length < MIN_ROWS_PER_GROUP ) {
            _newRowGroup(MIN_ROWS_PER_GROUP);
        }
        else if( length <= NUM_ROWS_PER_ROW_GROUP ) {
            //Allocate a smaller rowset, just to avoid completing buffers all the time
            _newRowGroup(length);
        }
        else {
            while( length > NUM_ROWS_PER_ROW_GROUP ) {
                //Write cur buff, allocating as needed and advancing rg_offset
                this->write(data, NUM_ROWS_PER_ROW_GROUP);
                //Advance data
                data   += NUM_ROWS_PER_ROW_GROUP;
                length -= NUM_ROWS_PER_ROW_GROUP;
            }
        }
    }


    for( int i=0; i< length; i++  ) {
        pre_neuron_id[i] = data[i].getPreNeuronID();
        post_neuron_id[i] = data[i].getPostNeuronID();
        pre_offset[i] = data[i].pre_offset;
        post_offset[i] = data[i].post_offset;
        distance_soma[i] = data[i].distance_soma;
        branch_order[i] = data[i].branch;
    #if COMBINE_SECTION_SEGMENT_FIELDS
        pre_post_sect_segm[i] =  ( & 0x11) +
                                ((data[i].pre_synapse_ids[SEGMENT_ID] & 0x11) << 8) +
                                ((data[i].pre_synapse_ids[SECTION_ID] & 0x11) << 16) +
                                ((data[i].pre_synapse_ids[SECTION_ID] & 0x11) << 24);
    #else
        pre_section[i] = data[i].pre_synapse_ids[SECTION_ID];
        pre_segment[i] = data[i].pre_synapse_ids[SEGMENT_ID];
        post_section[i] = data[i].post_synapse_ids[SECTION_ID];
        post_segment[i] = data[i].post_synapse_ids[SEGMENT_ID];
        if( pre_section[i]>0x7fff ) printf("Problematic pre_section %d\n", pre_section[i]);
        if( pre_segment[i]>0x7fff ) printf("Problematic pre_segment %d\n", pre_segment[i]);
        if( post_section[i]>0x7fff ) printf("Problematic post_section %d\n", post_section[i]);
        if( post_segment[i]>0x7fff ) printf("Problematic post_segment %d\n", post_segment[i]);
    #endif

    }

    //pre_neuron_id
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, pre_neuron_id);

    //post_neuron_id
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, post_neuron_id);

#if COMBINE_SECTION_SEGMENT_FIELDS
    //pre_post_sect_segm
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, pre_post_sect_segm);
#else
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, pre_section);
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, pre_segment);
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, post_section);
    int32_writer = static_cast<Int32Writer*>(rg_writer->NextColumn());
    int32_writer->WriteBatch(length, nullptr, nullptr, post_segment);
#endif

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

    rg_offset = rg_offset + length;
}















