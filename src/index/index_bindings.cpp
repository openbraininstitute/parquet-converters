#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <highfive/H5File.hpp>
#include <mpi.h>
#include "index.h"

namespace nb = nanobind;

const char* GROUP = "data";

void init_mpi() {
    int initialized;
    MPI_Initialized(&initialized);
    if (!initialized) {
        MPI_Init(nullptr, nullptr);
    }
}

void write_index(const std::string& filename, uint64_t sourceNodeCount, uint64_t targetNodeCount) {
    try {
        // Use PHDF5 for parallel I/O
        HighFive::FileAccessProps fapl;
        fapl.add(HighFive::MPIOFileAccess(MPI_COMM_WORLD, MPI_INFO_NULL));
        HighFive::File file(filename, HighFive::File::ReadWrite | HighFive::File::Create, fapl);
        HighFive::Group root = file.getGroup("/");
        
        if (!root.exist(GROUP)) {
            throw std::runtime_error("Group '" + std::string(GROUP) + "' not found in file");
        }
        
        HighFive::Group dataGroup = root.getGroup(GROUP);
        
        if (!dataGroup.exist("source_node_id")) {
            throw std::runtime_error("Dataset 'source_node_id' not found in group '" + std::string(GROUP) + "'");
        }
        
        if (!dataGroup.exist("target_node_id")) {
            throw std::runtime_error("Dataset 'target_node_id' not found in group '" + std::string(GROUP) + "'");
        }
        
        indexing::write(dataGroup, sourceNodeCount, targetNodeCount);
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Error in write_index: ") + e.what());
    }
}

NB_MODULE(index_writer_py, m) {
    m.def("init_mpi", &init_mpi, "Initialize MPI if not already initialized");
    m.def("write", &write_index, "Write index to HDF5 file using parallel I/O",
          nb::arg("filename"), nb::arg("sourceNodeCount"), nb::arg("targetNodeCount"));
}
