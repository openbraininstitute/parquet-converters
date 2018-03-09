#include "syn2_circuit.h"

namespace neuron_parquet {
namespace circuit {

namespace H5 = ::HighFive;




BaseSyn2CircuitHdf5::BaseSyn2CircuitHdf5(H5::File&& f, const string& population_name)
  : file_(f),
    properties_group_(create_base_groups(file_, population_name))
{ }


H5::Group BaseSyn2CircuitHdf5::create_base_groups(H5::File& f, const string& population_name) {
    H5::Group g1 = f.createGroup(string("synapses"));
    H5::Group g2 = g1.createGroup(population_name);
    H5::Group g3 = g2.createGroup(string("properties"));
    return g3;
}


BaseSyn2CircuitHdf5::Dataset BaseSyn2CircuitHdf5::get_dataset(int i) {
    return datasets_[i];
}

BaseSyn2CircuitHdf5::Dataset::Dataset(hid_t h5_loc, const string& name, hid_t h5type, uint32_t length) {
    hsize_t dims[] = { length };
    dspace = H5Screate_simple(1, dims, NULL);
    hid_t ds_id = H5Dcreate2(h5_loc, name.c_str(), h5type, dspace,
                             H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
}

ParallelSyn2CircuitHdf5::Dataset::Dataset(hid_t h5_loc, const string& name, hid_t h5type, uint32_t length)
  : BaseSyn2CircuitHdf5::Dataset(h5_loc, name, h5type, length)
{
    plist = H5Pcreate(H5P_DATASET_XFER);
}


}} // neuron_cpp::circuit
