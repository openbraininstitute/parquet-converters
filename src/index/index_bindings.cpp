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

nb::object get_comm_world() {
    return nb::capsule(MPI_COMM_WORLD, "mpi4py.MPI.Comm");
}

void write_index(const std::string& filename, uint64_t sourceNodeCount, uint64_t targetNodeCount, nb::object py_comm) {
    MPI_Comm* comm_ptr = static_cast<MPI_Comm*>(PyCapsule_GetPointer(py_comm.ptr(), "mpi4py.MPI.Comm"));
    if (!comm_ptr) {
        throw std::runtime_error("Failed to extract MPI_Comm from Python object");
    }
    MPI_Comm comm = *comm_ptr;
    
    // Use PHDF5 for parallel I/O
    HighFive::FileAccessProps fapl;
    fapl.add(HighFive::MPIOFileAccess(comm));
    HighFive::File file(filename, HighFive::File::ReadWrite | HighFive::File::Create, fapl);
    HighFive::Group root = file.getGroup("/");
    
    indexing::write(root, sourceNodeCount, targetNodeCount);
}

NB_MODULE(index_writer_py, m) {
    m.def("init_mpi", &init_mpi, "Initialize MPI if not already initialized");
    m.def("get_comm_world", &get_comm_world, "Get MPI_COMM_WORLD as a Python object");
    m.def("write", &write_index, "Write index to HDF5 file using parallel I/O",
          nb::arg("filename"), nb::arg("sourceNodeCount"), nb::arg("targetNodeCount"), nb::arg("comm"));
}
