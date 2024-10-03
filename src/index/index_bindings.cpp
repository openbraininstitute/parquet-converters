#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <highfive/H5File.hpp>
#include <highfive/H5FileDriver.hpp>
#include <mpi.h>
#include "index.h"

namespace nb = nanobind;

void init_mpi() {
    int initialized;
    MPI_Initialized(&initialized);
    if (!initialized) {
        MPI_Init(nullptr, nullptr);
    }
}

void write_index(const std::string& filename, uint64_t sourceNodeCount, uint64_t targetNodeCount, nb::object py_comm) {
    MPI_Comm comm = MPI_COMM_WORLD;  // Default to MPI_COMM_WORLD

    if (!py_comm.is_none()) {
        void* comm_ptr = PyCapsule_GetPointer(py_comm.ptr(), "mpi4py.MPI.Comm");
        if (comm_ptr) {
            comm = *static_cast<MPI_Comm*>(comm_ptr);
        } else {
            PyErr_Clear();  // Clear any Python error set by PyCapsule_GetPointer
        }
    }

    // Use PHDF5 for parallel I/O
    HighFive::FileAccessProps fapl;
    fapl.add(HighFive::MPIOFileAccess(comm, MPI_INFO_NULL));
    HighFive::File file(filename, HighFive::File::ReadWrite | HighFive::File::Create, fapl);
    HighFive::Group root = file.getGroup("/");
    
    indexing::write(root, sourceNodeCount, targetNodeCount);
}

NB_MODULE(index_writer_py, m) {
    m.def("init_mpi", &init_mpi, "Initialize MPI if not already initialized");
    m.def("write", &write_index, "Write index to HDF5 file using parallel I/O",
          nb::arg("filename"), nb::arg("sourceNodeCount"), nb::arg("targetNodeCount"), nb::arg("comm") = nb::none());
}
