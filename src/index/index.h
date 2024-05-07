#pragma once

#include <highfive/H5Group.hpp>

namespace indexing {

void write(HighFive::Group& h5Root,
           uint64_t sourceNodeCount,
           uint64_t targetNodeCount);

} // namespace index
