#ifndef SYN2_UTIL_H
#define SYN2_UTIL_H

#include <hdf5.h>
#include <string>

namespace neuron_parquet {
namespace circuit {

using std::string;

const char* DEFAULT_SYN2_DS_PATH = "/synapses/default/properties/";


class Syn2CircuitHdf5 {
public:

    Syn2CircuitHdf5(const string& filepath) {
        circuit = H5Fcreate(filepath.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    }

    ~Syn2CircuitHdf5() {
        H5Fclose(circuit);
    }

    void link_existing_dataset(const string& filepath, const string& dataset_name)  {
        const string dst (string(DEFAULT_SYN2_DS_PATH) + filepath);
        H5Lcreate_external(dst.c_str(), dataset_name.c_str(), circuit, dataset_name.c_str(), H5P_DEFAULT, H5P_DEFAULT);
    }



private:
    hid_t circuit;
};


}}



#endif // SYN2_UTIL_H
