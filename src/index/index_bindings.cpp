#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <highfive/H5File.hpp>
#include "index.h"

namespace nb = nanobind;

NB_MODULE(index_writer_py, m) {
    m.def("write", [](const std::string& filename, uint64_t sourceNodeCount, uint64_t targetNodeCount) {
        HighFive::File file(filename, HighFive::File::ReadWrite);
        HighFive::Group root = file.getGroup("/");
        indexing::write(root, sourceNodeCount, targetNodeCount);
    }, "Write index to HDF5 file", nb::arg("filename"), nb::arg("sourceNodeCount"), nb::arg("targetNodeCount"));
}
