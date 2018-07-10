/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#include <vector>
#include <iostream>
#include <syn2/synapses_writer.hpp>

#include <neuron_parquet/circuit.h>
#include <progress.hpp>


using namespace neuron_parquet;
using namespace neuron_parquet::circuit;

using utils::ProgressMonitor;
using std::vector;
using std::string;

void convert_circuit(const vector<string> & filenames)  {
    CircuitMultiReaderParquet reader(filenames) ;
    CircuitWriterSYN2 writer(string("circuit.syn2"), reader.record_count());

    Converter<CircuitData> converter( reader, writer );

    ProgressMonitor p(reader.block_count());
    converter.setProgressHandler(p);

    converter.exportAll();

    // Check for datasets with required name for SYN2
    Syn2CircuitHdf5& syn2circuit = writer.syn2_file();
 
    // SYN2 required fields might have a different name
    if(!syn2circuit.has_dataset("connected_neurons_pre")) {
        std::unordered_map<string, string> mapping {
            { string("pre_neuron_id"),  string("connected_neurons_pre")  },
            { string("post_neuron_id"), string("connected_neurons_post") },
            { string("pre_gid"),  string("connected_neurons_pre")  },
            { string("post_gid"), string("connected_neurons_post") },
        };
        for(auto map_pair : mapping) {
            if(syn2circuit.has_dataset(map_pair.first)) {
                syn2circuit.link_dataset(map_pair.second, map_pair.first);
            }
        }
    }
    
}

int main(int argc, char* argv[]) {
    if(argc < 2) {
        std::cout << "Please provide files to convert" << std::endl;
        return -1;
    }

    vector<string> names(argc-1);

    for(int i=1; i<argc; i++) {
        names[i-1] = string(argv[i]);
    }

    convert_circuit(names);

    std::cout << "\nData copy complete. Creating SYN2 indexes..." << std::endl;

    syn2::synapses_writer writer(string("circuit.syn2"));
    writer.create_all_index();

    std::cout << "\nComplete." << std::endl;

    return 0;
}
