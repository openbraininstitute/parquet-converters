#include "syn2_circuit.h"

namespace neuron_parquet {
namespace circuit {

namespace H5 = ::HighFive;


Syn2CircuitHdf5::Syn2CircuitHdf5(const string& filepath, const string &population_name, uint64_t n_records)
  : parallel_mode_(false),
    file_(H5::File(filepath, H5::File::Create|H5::File::Truncate)),
    properties_group_(create_base_groups(file_, population_name)),
    n_records_(n_records)
{ }

Syn2CircuitHdf5::Syn2CircuitHdf5(const string& filepath, const string &population_name,
                                 const MPI_Comm& mpicomm, const MPI_Info& mpiinfo, uint64_t n_records)
  : parallel_mode_(true),
    file_(H5::File(filepath, H5::File::Create|H5::File::Truncate, H5::MPIOFileDriver(mpicomm, mpiinfo))),
    properties_group_(create_base_groups(file_, population_name)),
    n_records_(n_records)
{ }


H5::Group Syn2CircuitHdf5::create_base_groups(H5::File& f, const string& population_name) {
    H5::Group g1 = f.createGroup(string("synapses"));
    H5::Group g2 = g1.createGroup(population_name);
    H5::Group g3 = g2.createGroup(string("properties"));
    return g3;
}


void Syn2CircuitHdf5::create_dataset(const string& name, hid_t h5type, uint64_t length) {
    if(length==0) length = n_records_;
    assert( length>0 );
    if( datasets_.count(name) >0 ) {
        throw std::runtime_error("Attempt to create an existing dataset dataset: " + name);
    }
    datasets_[name] = Dataset(properties_group_.getId(), name, h5type, length, parallel_mode_);
}


Syn2CircuitHdf5::Dataset::Dataset(hid_t h5_loc, const string& name, hid_t h5type, uint64_t length, bool parallel) {
    hsize_t dims[] = { length };
    dspace = H5Screate_simple(1, dims, NULL);
    ds = H5Dcreate2(h5_loc, name.c_str(), h5type, dspace,
                    H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if(parallel) {
        plist = H5Pcreate(H5P_DATASET_XFER);
        H5Pset_dxpl_mpio(plist, H5FD_MPIO_INDEPENDENT);
    }
    else {
        plist = H5P_DEFAULT;
    }
    dtype = h5type;
    valid_.reset(new bool);
}



Syn2CircuitHdf5::Dataset::~Dataset() {
    if( ! valid_ ) {
        return;
    }

    H5Dclose(ds);
    H5Sclose(dspace);
    if(plist != H5P_DEFAULT) {
        H5Pclose(plist);
    }
}


void Syn2CircuitHdf5::Dataset::write(const void *buffer, const hsize_t buf_len, const hsize_t h5offset) {
    hid_t memspace = H5Screate_simple(1, &buf_len, NULL);
    H5Sselect_hyperslab(dspace, H5S_SELECT_SET, &h5offset, NULL, &buf_len, NULL);
    H5Dwrite(ds, dtype, memspace, dspace, plist, buffer);
    H5Sclose(memspace);
}


}} // neuron_cpp::circuit
