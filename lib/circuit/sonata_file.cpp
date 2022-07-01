/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#include <unordered_set>
#include <syn2/indexing.hpp>
#include "sonata_file.h"

namespace neuron_parquet {
namespace circuit {


SonataFile::SonataFile(const std::string& filepath, const std::string &population_name, uint64_t n_records)
  : parallel_mode_(false),
    file_(HighFive::File(filepath, HighFive::File::Create|HighFive::File::Truncate)),
    population_group_(file_.createGroup("edges").createGroup(population_name)),
    properties_group_(population_group_.createGroup("0")),
    n_records_(n_records)
{
}

#ifdef NEURONPARQUET_USE_MPI
SonataFile::SonataFile(const std::string& filepath, const std::string &population_name,
                                 const MPI_Comm& mpicomm, const MPI_Info& mpiinfo, uint64_t n_records)
  : parallel_mode_(true),
    file_(HighFive::File(filepath, HighFive::File::Create|HighFive::File::Truncate, HighFive::MPIOFileDriver(mpicomm, mpiinfo))),
    population_group_(file_.createGroup("edges").createGroup(population_name)),
    properties_group_(population_group_.createGroup("0")),
    n_records_(n_records)
{
}
#endif

void SonataFile::create_dataset(const std::string& name,
                                     hid_t h5type,
                                     uint64_t length,
                                     uint64_t width) {
    const static std::unordered_set<std::string> TOPLEVEL_DATASETS{
        "edge_type_id",
        "source_node_id",
        "target_node_id"
    };

    if(length==0) length = n_records_;
    if( datasets_.count(name) >0 ) {
        throw std::runtime_error("Attempt to create an existing dataset dataset: " + name);
    }

    if (TOPLEVEL_DATASETS.count(name) > 0) {
        datasets_[name] = Dataset(population_group_.getId(), name, h5type, length, width, parallel_mode_);
    } else {
        datasets_[name] = Dataset(properties_group_.getId(), name, h5type, length, width, parallel_mode_);
    }
}

void SonataFile::create_attribute(const std::string& name, const std::string& value) {
    auto attr = population_group_.createAttribute<std::string>(name, HighFive::DataSpace::From(value));
    attr.write(value);
}

void SonataFile::create_dataset_attribute(const std::string& dataset, const std::string& name, const std::string& value) {
    auto attr = population_group_.getDataSet(dataset).createAttribute<std::string>(name, HighFive::DataSpace::From(value));
    attr.write(value);
}

void SonataFile::create_library(const std::string& name, const std::vector<std::string>& data) {
    properties_group_.createDataSet("@library/" + name, data);
}

void SonataFile::write_indices(size_t source_size, size_t target_size, bool parallel) {
    auto indices_group = population_group_.createGroup("indices");
    auto source_index = indices_group.createGroup("source_to_target");
    auto target_index = indices_group.createGroup("target_to_source");
    auto fun = &edge::create_index;
#ifdef NEURONPARQUET_USE_MPI
    if (parallel) {
        fun = &edge::create_index_mpi;
    }
#endif
    fun(
        population_group_.getDataSet("source_node_id"),
        source_index,
        "node_id_to_ranges",
        "range_to_edge_id",
        true,
        source_size
    );
    fun(
        population_group_.getDataSet("target_node_id"),
        target_index,
        "node_id_to_ranges",
        "range_to_edge_id",
        true,
        target_size
    );
}

SonataFile::Dataset::Dataset(hid_t h5_loc,
                                  const std::string& name,
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



SonataFile::Dataset::~Dataset() {
    if( ! valid_ ) {
        return;
    }

    H5Sclose(dspace);
    H5Dclose(ds);
    if(plist != H5P_DEFAULT) {
        H5Pclose(plist);
    }
}


void SonataFile::Dataset::write(const void *buffer,
                                     const hsize_t length,
                                     const hsize_t offset) {
    hid_t memspace = H5Screate_simple(1, &length, NULL);
    H5Sselect_hyperslab(dspace, H5S_SELECT_SET, &offset, NULL, &length, NULL);
    H5Dwrite(ds, dtype, memspace, dspace, plist, buffer);
    H5Sclose(memspace);
}

void SonataFile::Dataset::write(const void *buffer,
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
