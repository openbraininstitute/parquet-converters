#ifndef SYN2_UTIL_H
#define SYN2_UTIL_H

#include <stdexcept>
#include <string>
#include <unordered_map>
#include <hdf5.h>
#include <highfive/H5File.hpp>


namespace neuron_parquet {
namespace circuit {

namespace H5 = ::HighFive;


class Syn2CircuitHdf5 {
public:
    class Dataset;
    typedef std::string string;

    Syn2CircuitHdf5(const string& filepath, const string& population_name, uint64_t n_records=0);
    Syn2CircuitHdf5(const string& filepath, const string& population_name,
                    const MPI_Comm& mpicomm, const MPI_Info& mpiinfo, uint64_t n_records=0);

    ~Syn2CircuitHdf5() {}


    void create_dataset(const string& name, hid_t h5type, uint64_t length=0);

    inline void link_existing_dataset(const string& circuit_syn2_path, const string& dataset_name, const string& link_name) {
        H5Lcreate_external(circuit_syn2_path.c_str(), dataset_name.c_str(), properties_group_.getId(), link_name.c_str(),
                           H5P_DEFAULT, H5P_DEFAULT);
    }

    inline void link_existing_dataset(const string& filepath, const string& dataset_name)  {
        link_existing_dataset(filepath, dataset_name, dataset_name);
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


    class Dataset {
    public:
        Dataset(hid_t h5_loc, const string& name, hid_t h5type, uint64_t length,
                bool parallel=false);
        Dataset() {}
        ~Dataset();

        // no copies (we hold ids for h5 objects and must know when to destroy them)
        Dataset(const Dataset&) = delete;
        Dataset& operator=(const Dataset&) = delete;
        // Move ok given we invalidate the old one
        Dataset& operator=(Dataset&&) = default;
        Dataset(Dataset&&) = default;

        void write(const void* buffer, const hsize_t buf_len, const hsize_t h5offset);

    protected:
        hid_t ds, dspace, plist, dtype;
        // Keep control after moves if this is a valid object
        // Base types are not reset, but unique_ptr's are.
        std::unique_ptr<void> valid_;
    };


protected:
    Syn2CircuitHdf5() = delete;
    static H5::Group create_base_groups(H5::File& f, const string& population_name);

    bool parallel_mode_;
    H5::File file_;
    H5::Group properties_group_;
    uint64_t n_records_;
    std::unordered_map<string, Dataset> datasets_;
};




}} //NS



#endif // SYN2_UTIL_H
