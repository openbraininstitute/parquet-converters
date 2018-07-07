/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#ifndef SYN2_UTIL_H
#define SYN2_UTIL_H

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

namespace H5 = ::HighFive;


class Syn2CircuitHdf5 {
public:
    class Dataset;
    typedef std::string string;

    Syn2CircuitHdf5(const string& filepath, const string& population_name, uint64_t n_records=0);
#ifdef NEURONPARQUET_USE_MPI
    Syn2CircuitHdf5(const string& filepath, const string& population_name,
                    const MPI_Comm& mpicomm, const MPI_Info& mpiinfo, uint64_t n_records=0);
#endif

    Syn2CircuitHdf5(Syn2CircuitHdf5&&) = default;
    ~Syn2CircuitHdf5() {}


    void create_dataset(const string& name, hid_t h5type, uint64_t length=0);

    inline void link_external_dataset(const string& circuit_syn2_path, const string& dataset_name, const string& link_name) {
        H5::Group properties_group = population_group_.getGroup("properties");
        H5Lcreate_external(circuit_syn2_path.c_str(), dataset_name.c_str(), properties_group.getId(), link_name.c_str(),
                           H5P_DEFAULT, H5P_DEFAULT);
    }

    inline void link_external_dataset(const string& filepath, const string& dataset_name)  {
        link_external_dataset(filepath, dataset_name, dataset_name);
    }

    inline void link_dataset(const string& link_name, const string& dataset_name)  {
        H5::Group propg = population_group_.getGroup("properties");
        H5Lcreate_hard(propg.getId(), dataset_name.c_str(), propg.getId(), link_name.c_str(), H5P_DEFAULT, H5P_DEFAULT);
    }

    inline const std::unordered_map<string, Dataset>& datasets() {
        return datasets_;
    }

    inline bool has_dataset(const string& name) {
        return datasets_.count(name) > 0;
    }

    inline Dataset& operator[](const string& name) {
        return datasets_.at(name);
    }

    /**
     * @brief The Dataset class
     *        A relativelly low-level wrapper to Hdf5 SYN2 datasets, optimized for many small-chunk writing
     *        The dataset is not resizable, but can be written in chunks given the buffer length and destination offset
     */
    class Dataset {
    public:
        Dataset(hid_t h5_loc, const string& name, hid_t h5type, uint64_t length,
                bool parallel=false);
        Dataset() {}
        ~Dataset();

        // no copies (we hold ids for h5 objects and must know when to destroy them)
        Dataset(const Dataset&) = delete;
        Dataset& operator=(const Dataset&) = delete;
        // Move ok given we track invalidation (the old doesnt close the ids)
        Dataset& operator=(Dataset&&) = default;
        Dataset(Dataset&&) = default;

        void write(const void* buffer, const hsize_t buf_len, const hsize_t h5offset);

    protected:
        hid_t ds, dspace, plist, dtype;
        // Keep control after moves if this is a valid object
        // unique_ptr's work, they init as "false" and become "false" after moved.
        std::unique_ptr<bool> valid_;
    };


protected:
    Syn2CircuitHdf5() = default;

    static H5::Group create_base_groups(H5::File& f, const string& population_name);

    bool parallel_mode_;
    H5::File file_;
    H5::Group population_group_;
    uint64_t n_records_;
    std::unordered_map<string, Dataset> datasets_;
};




}} //NS



#endif // SYN2_UTIL_H
