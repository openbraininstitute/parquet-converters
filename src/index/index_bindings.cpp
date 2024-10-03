#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <highfive/H5File.hpp>
#include <mpi.h>
#include <mpi4py/mpi4py.h>
#include "index.h"

namespace nb = nanobind;

void init_mpi() {
    int initialized;
    MPI_Initialized(&initialized);
    if (!initialized) {
        MPI_Init(nullptr, nullptr);
    }
    import_mpi4py();
}

nb::object get_comm_world() {
    return nb::capsule(MPI_COMM_WORLD, "mpi4py.MPI.Comm");
}

void write_index(const std::string& filename, uint64_t sourceNodeCount, uint64_t targetNodeCount, nb::object py_comm) {
    if (!PyObject_TypeCheck(py_comm.ptr(), &PyMPIComm_Type)) {
        throw std::runtime_error("Expected an mpi4py.MPI.Comm object");
    }
    MPI_Comm comm = *PyMPIComm_Get(py_comm.ptr());
    
    // Use PHDF5 for parallel I/O
    HighFive::File file(filename, HighFive::File::ReadWrite | HighFive::File::Create, HighFive::MPIOFileDriver(comm));
    HighFive::Group root = file.getGroup("/");
    
    indexing::write(root, sourceNodeCount, targetNodeCount);
}

NB_MODULE(index_writer_py, m) {
    m.def("init_mpi", &init_mpi, "Initialize MPI if not already initialized");
    m.def("get_comm_world", &get_comm_world, "Get MPI_COMM_WORLD as a Python object");
    m.def("write", &write_index, "Write index to HDF5 file using parallel I/O",
          nb::arg("filename"), nb::arg("sourceNodeCount"), nb::arg("targetNodeCount"), nb::arg("comm"));
}
