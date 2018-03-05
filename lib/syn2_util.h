#ifndef SYN2_UTIL_H
#define SYN2_UTIL_H

#include <stdexcept>
#include <string>
#include <hdf5.h>


namespace neuron_parquet {
namespace circuit {

using std::string;

const char* DEFAULT_SYN2_DS_PATH = "/synapses/default/properties/";


class Syn2CircuitHdf5 {
public:

    Syn2CircuitHdf5(const string& filepath)   {
        circuit = H5Fcreate(filepath.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
        if( ! H5Iis_valid(circuit)) {
            throw std::runtime_error("Cant open circuit file for writing");
        }


        hid_t g = H5Gcreate2(circuit, "/synapses", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        if( ! H5Iis_valid(circuit)) {
            throw std::runtime_error("Cant create properties group");
        }
        H5Gclose(g);

        g = H5Gcreate2(circuit, "/synapses/default", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Gclose(g);

        g = H5Gcreate2(circuit, DEFAULT_SYN2_DS_PATH, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Gclose(g);
    }

    ~Syn2CircuitHdf5() {
        H5Fclose(circuit);
    }

    void link_existing_dataset(const string& circuit_syn2_path, const string& dataset_name, const string& link_name)  {
        const string src_ds_name (string(DEFAULT_SYN2_DS_PATH) + link_name);
        H5Lcreate_external(circuit_syn2_path.c_str(), dataset_name.c_str(), circuit, src_ds_name.c_str(), H5P_DEFAULT, H5P_DEFAULT);
    }

    void link_existing_dataset(const string& filepath, const string& dataset_name)  {
        link_existing_dataset(filepath, dataset_name, dataset_name);
    }



private:
    hid_t circuit;
};


}} //NS



#endif // SYN2_UTIL_H
