#ifndef NRN_DEFS_H
#define NRN_DEFS_H

#include <arrow/table.h>

namespace neuron_parquet {
namespace circuit {


//static const char* FIELDS_REQUIRED[] = {"pre_gid", "post_gid"};

// Circuits will allow for a variable number of fields.
// Therefore the record cant have the fields statically
// Instead it will act as a protocol, where both readers and producers
// know it's not buffer and act accordingly

struct CircuitData {
    std::shared_ptr<arrow::Table> row_group;
};

}}  // ns nrn_parquet::circuit

#endif // NRN_DEFS_H
