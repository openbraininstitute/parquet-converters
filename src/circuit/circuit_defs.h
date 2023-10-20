/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#pragma once

#include <arrow/table.h>
#include <parquet/schema.h>
#include <parquet/metadata.h>

namespace neuron_parquet {
namespace circuit {


// Circuits will allow for a variable number of fields.
// Therefore the record cant have the fields statically
// Instead it will act as a protocol, where both readers and producers
// know it's not buffer and act accordingly

struct CircuitData {
    using Schema = parquet::SchemaDescriptor;
    using Metadata = parquet::KeyValueMetadata;
    std::shared_ptr<arrow::Table> row_group;
};

}  // namespace circuit
}  // namespace neuron_parquet
