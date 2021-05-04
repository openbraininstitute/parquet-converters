/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#pragma once

#include <iostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <hdf5.h>
#include <highfive/H5File.hpp>
#ifdef NEURONPARQUET_USE_MPI
#include <mpi.h>
#endif

namespace neuron_parquet {
namespace circuit {


class SonataFile {
public:
    class Dataset;

    SonataFile(const std::string& filepath, const std::string& population_name, uint64_t n_records=0);
#ifdef NEURONPARQUET_USE_MPI
    SonataFile(const std::string& filepath, const std::string& population_name,
                    const MPI_Comm& mpicomm, const MPI_Info& mpiinfo, uint64_t n_records=0);
#endif

    SonataFile(SonataFile&&) = default;
    ~SonataFile() = default;

    void write_indices(size_t source_size, size_t target_size, bool parallel=false);

    void create_attribute(const std::string& name, const std::string& value);
    void create_dataset_attribute(const std::string& dataset, const std::string& name, const std::string& value);

    void create_dataset(const std::string& name, hid_t h5type, uint64_t length=0, uint64_t width=1);

    inline const std::unordered_map<std::string, Dataset>& datasets() {
        return datasets_;
    }

    inline bool has_dataset(const std::string& name) {
        return datasets_.count(name) > 0;
    }

    inline Dataset& operator[](const std::string& name) {
        return datasets_.at(name);
    }

    /**
     * @brief The Dataset class
     *        A relativelly low-level wrapper to Hdf5 datasets, optimized for many small-chunk writing
     *        The dataset is not resizable, but can be written in chunks given the buffer length and destination offset
     */
    class Dataset {
    public:
        Dataset(hid_t h5_loc, const std::string& name, hid_t h5type, uint64_t length,
                uint64_t width=1, bool parallel=false);
        Dataset() {}
        ~Dataset();

        // no copies (we hold ids for h5 objects and must know when to destroy them)
        Dataset(const Dataset&) = delete;
        Dataset& operator=(const Dataset&) = delete;
        // Move ok given we track invalidation (the old doesnt close the ids)
        Dataset& operator=(Dataset&&) = default;
        Dataset(Dataset&&) = default;

        void write(const void* buffer,
                   const hsize_t length,
                   const hsize_t h5offset);
        void write(const void* buffer,
                   const hsize_t column,
                   const hsize_t length,
                   const hsize_t h5offset);

    protected:
        hid_t ds, plist, dspace, dtype;
        uint64_t width;
        // Keep control after moves if this is a valid object
        // unique_ptr's work, they init as "false" and become "false" after moved.
        std::unique_ptr<bool> valid_;
    };


protected:
    SonataFile() = default;

    bool parallel_mode_;
    HighFive::File file_;
    HighFive::Group population_group_;
    HighFive::Group properties_group_;
    uint64_t n_records_;
    std::unordered_map<std::string, Dataset> datasets_;
};

}} //NS
