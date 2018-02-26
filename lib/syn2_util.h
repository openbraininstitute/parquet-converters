#ifndef SYN2_UTIL_H
#define SYN2_UTIL_H

#include <hdf5.h>
#include <string>

namespace neuron_parquet {
namespace syn2 {
using std::string;



class CircuitHdf5 {
public:
    CircuitHdf5(const string& filepath) {
        circuit = H5Fcreate(filepath.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    }

    void link_existing_dataset(const string& filepath, const string& dataset_name)  {
        const string dst = DEFAULT_DATASET_PATH + filepath;
        H5Lcreate_external(dst.c_str(), dataset_name.c_str(), circuit, dataset_name.c_str(), H5P_DEFAULT, H5P_DEFAULT);
    }


    static const string DEFAULT_SYN2_DS_PATH = "/synapses/default/properties/";

private:
    hid_t circuit;
};


}}



#endif // SYN2_UTIL_H
