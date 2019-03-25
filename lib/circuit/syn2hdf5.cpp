/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#include "syn2hdf5.h"

namespace neuron_parquet {
namespace circuit {

namespace H5 = ::HighFive;


Syn2CircuitHdf5::Syn2CircuitHdf5(const string& filepath, const string &population_name, uint64_t n_records)
  : parallel_mode_(false),
    file_(H5::File(filepath, H5::File::Create|H5::File::Truncate)),
    population_group_(create_base_groups(file_, population_name)),
    n_records_(n_records)
{ }

#ifdef NEURONPARQUET_USE_MPI
Syn2CircuitHdf5::Syn2CircuitHdf5(const string& filepath, const string &population_name,
                                 const MPI_Comm& mpicomm, const MPI_Info& mpiinfo, uint64_t n_records)
  : parallel_mode_(true),
    file_(H5::File(filepath, H5::File::Create|H5::File::Truncate, H5::MPIOFileDriver(mpicomm, mpiinfo))),
    population_group_(create_base_groups(file_, population_name)),
    n_records_(n_records)
{ }
#endif

H5::Group Syn2CircuitHdf5::create_base_groups(H5::File& f, const string& population_name) {
    H5::Group g1 = f.createGroup(string("synapses"));
    H5::Group g2 = g1.createGroup(population_name);
    H5::Group g3 = g2.createGroup(string("properties"));
    return g2;
}


void Syn2CircuitHdf5::create_dataset(const string& name,
                                     hid_t h5type,
                                     uint64_t length,
                                     uint64_t width) {
    if(length==0) length = n_records_;
    assert( length>0 );
    if( datasets_.count(name) >0 ) {
        throw std::runtime_error("Attempt to create an existing dataset dataset: " + name);
    }
    H5::Group properties_group = population_group_.getGroup("properties");
    datasets_[name] = Dataset(properties_group.getId(), name, h5type, length, width, parallel_mode_);
}



Syn2CircuitHdf5::Dataset::Dataset(hid_t h5_loc,
                                  const string& name,
                                  hid_t h5type,
                                  uint64_t length,
                                  uint64_t w,
                                  bool parallel)
        : width(w) {
    std::vector<hsize_t> dims{length};
    if (width > 1)
        dims.push_back(width);
    dspace = H5Screate_simple(dims.size(), dims.data(), NULL);
    ds = H5Dcreate2(h5_loc, name.c_str(), h5type, dspace,
                    H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
#ifdef NEURONPARQUET_USE_MPI
    if(parallel) {
        plist = H5Pcreate(H5P_DATASET_XFER);
        H5Pset_dxpl_mpio(plist, H5FD_MPIO_INDEPENDENT);
    }
    else
#endif
    {
        (void) parallel;
        plist = H5P_DEFAULT;
    }
    dtype = h5type;
    valid_.reset(new bool);
}



Syn2CircuitHdf5::Dataset::~Dataset() {
    if( ! valid_ ) {
        return;
    }

    H5Sclose(dspace);
    H5Dclose(ds);
    if(plist != H5P_DEFAULT) {
        H5Pclose(plist);
    }
}


void Syn2CircuitHdf5::Dataset::write(const void *buffer,
                                     const hsize_t length,
                                     const hsize_t offset) {
    hid_t memspace = H5Screate_simple(1, &length, NULL);
    H5Sselect_hyperslab(dspace, H5S_SELECT_SET, &offset, NULL, &length, NULL);
    H5Dwrite(ds, dtype, memspace, dspace, plist, buffer);
    H5Sclose(memspace);
}

void Syn2CircuitHdf5::Dataset::write(const void *buffer,
                                     const hsize_t column,
                                     const hsize_t length,
                                     const hsize_t offset) {
    std::array<hsize_t, 2> sizes{length, 1};
    std::array<hsize_t, 2> start{offset, column};

    hid_t memspace = H5Screate_simple(2, sizes.data(), NULL);
    H5Sselect_hyperslab(dspace, H5S_SELECT_SET, start.data(), NULL, sizes.data(), NULL);
    H5Dwrite(ds, dtype, memspace, dspace, plist, buffer);
    H5Sclose(memspace);
}


}} // neuron_cpp::circuit
