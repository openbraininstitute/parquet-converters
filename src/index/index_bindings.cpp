#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <highfive/H5File.hpp>
#include <mpi.h>
#include "index.h"
#include <iostream>
#include <filesystem>

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
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    std::cout << "Rank " << rank << "/" << size << ": Entering write_index" << std::endl;
    std::cout.flush();
    
    try {
        std::cout << "Rank " << rank << "/" << size << ": Setting up PHDF5" << std::endl;
        std::cout.flush();
        std::cout << "Rank " << rank << "/" << size << ": Opening file: " << filename << std::endl;
        std::cout.flush();
        
        // Check if the file exists
        if (!std::filesystem::exists(filename)) {
            std::cerr << "Rank " << rank << "/" << size << ": File " << filename << " does not exist" << std::endl;
            std::cerr.flush();
            throw std::runtime_error("File does not exist: " + filename);
        }
        std::cout << "Rank " << rank << "/" << size << ": File exists: true" << std::endl;
        std::cout.flush();

        // Open the file in read-write mode without MPI-IO
        HighFive::File file(filename, HighFive::File::ReadWrite);
        
        std::cout << "Rank " << rank << "/" << size << ": File opened successfully" << std::endl;
        std::cout.flush();
        
        std::cout << "Rank " << rank << "/" << size << ": Checking for group: " << GROUP << std::endl;
        std::cout.flush();
        if (!file.exist(GROUP)) {
            std::cerr << "Rank " << rank << "/" << size << ": Group '" << GROUP << "' not found in file" << std::endl;
            std::cerr.flush();
            throw std::runtime_error("Group '" + std::string(GROUP) + "' not found in file");
        }
        
        std::cout << "Rank " << rank << "/" << size << ": Getting group" << std::endl;
        std::cout.flush();
        HighFive::Group dataGroup = file.getGroup(GROUP);
        
        std::cout << "Rank " << rank << "/" << size << ": Checking for datasets" << std::endl;
        std::cout.flush();
        if (!dataGroup.exist("source_node_id") || !dataGroup.exist("target_node_id")) {
            std::cerr << "Rank " << rank << "/" << size << ": Required datasets 'source_node_id' or 'target_node_id' not found in group '" << GROUP << "'" << std::endl;
            std::cerr.flush();
            throw std::runtime_error("Required datasets 'source_node_id' or 'target_node_id' not found in group '" + std::string(GROUP) + "'");
        }
        
        std::cout << "Rank " << rank << "/" << size << ": Calling indexing::write" << std::endl;
        std::cout.flush();
        indexing::write(dataGroup, sourceNodeCount, targetNodeCount);
        std::cout << "Rank " << rank << "/" << size << ": Finished indexing::write" << std::endl;
        std::cout.flush();
    } catch (const HighFive::Exception& e) {
        std::cerr << "Rank " << rank << "/" << size << ": HighFive error in write_index: " << e.what() << std::endl;
        std::cerr << "Error details: " << e.what() << std::endl;
        std::cerr.flush();
        throw;
    } catch (const std::exception& e) {
        std::cerr << "Rank " << rank << "/" << size << ": Error in write_index: " << e.what() << std::endl;
        std::cerr << "Error details: " << e.what() << std::endl;
        std::cerr.flush();
        throw;
    } catch (...) {
        std::cerr << "Rank " << rank << "/" << size << ": Unknown error in write_index" << std::endl;
        std::cerr.flush();
        throw;
    }
    
    std::cout << "Rank " << rank << "/" << size << ": Exiting write_index" << std::endl;
    std::cout.flush();
}

NB_MODULE(index_writer_py, m) {
    m.def("init_mpi", &init_mpi, "Initialize MPI if not already initialized");
    m.def("write", &write_index, "Write index to HDF5 file using parallel I/O",
          nb::arg("filename"), nb::arg("sourceNodeCount"), nb::arg("targetNodeCount"));
}
