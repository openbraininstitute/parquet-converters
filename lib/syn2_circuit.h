#ifndef SYN2_UTIL_H
#define SYN2_UTIL_H

#include <stdexcept>
#include <string>
#include <vector>
#include <hdf5.h>
#include <highfive/H5File.hpp>


namespace neuron_parquet {
namespace circuit {

namespace H5 = ::HighFive;


class BaseSyn2CircuitHdf5 {
protected:
    class Dataset;
    typedef std::string string;

public:
    BaseSyn2CircuitHdf5(H5::File&& f, const string& population_name);

    virtual ~BaseSyn2CircuitHdf5() {}

    void link_existing_dataset(const string& circuit_syn2_path,
                               const string& dataset_name,
                               const string& link_name)  {
        H5Lcreate_external(circuit_syn2_path.c_str(), dataset_name.c_str(),
                           properties_group_.getId(), link_name.c_str(),
                           H5P_DEFAULT, H5P_DEFAULT);
    }

    void link_existing_dataset(const string& filepath, const string& dataset_name)  {
        link_existing_dataset(filepath, dataset_name, dataset_name);
    }

    virtual Dataset get_dataset(int i);

protected:
    class Dataset {
    public:
        Dataset(hid_t h5_loc, const string& name, hid_t h5type, uint32_t length);
        Dataset() = delete;
        virtual ~Dataset() {
            H5Dclose(ds);
            H5Sclose(dspace);
        }
    protected:
        hid_t ds, dspace;
    };

    H5::File file_;
    H5::Group properties_group_;
    std::vector<Dataset> datasets_;

private:
    static H5::Group create_base_groups(H5::File& f, const string& population_name);
};



class Syn2CircuitHdf5 : public BaseSyn2CircuitHdf5 {
public:
    Syn2CircuitHdf5(const string& filepath, const string &population_name)
      : BaseSyn2CircuitHdf5(H5::File(filepath, H5::File::Create), population_name)
    {}

    int create_dataset(const string& name, hid_t h5type, uint32_t length) {
        datasets_.push_back(Dataset(properties_group_.getId(), name, h5type, length));
        return datasets_.size() - 1;
    }

    typedef BaseSyn2CircuitHdf5::Dataset Dataset;
};



class ParallelSyn2CircuitHdf5 : public BaseSyn2CircuitHdf5{
public:
    ParallelSyn2CircuitHdf5(const string& filepath, const string& population_name, MPI_Comm mpicomm, MPI_Info mpiinfo)
      : BaseSyn2CircuitHdf5(H5::File(filepath, H5::File::Create, H5::MPIOFileDriver(mpicomm, mpiinfo)), population_name)
    { }

    int create_dataset(const string& name, hid_t h5type, uint32_t length) {
        datasets_.push_back(Dataset(properties_group_.getId(), name, h5type, length));
        return datasets_.size() - 1;
    }

    class Dataset : public BaseSyn2CircuitHdf5::Dataset {
    public:
        Dataset(hid_t h5_loc, const string& name, hid_t h5type, uint32_t length);
        Dataset() = delete;
        virtual ~Dataset(){
            H5Pclose(plist);
        }
    private:
        hid_t plist;
    };
};



}} //NS



#endif // SYN2_UTIL_H
