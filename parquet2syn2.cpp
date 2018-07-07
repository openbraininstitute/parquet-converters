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


using namespace neuron_parquet;
using namespace neuron_parquet::circuit;

using namespace std;


void convert_circuit(const vector<string> & filenames, const string& output_filename)  {
    if(filenames.size() >= 100) {
        cout << "Retrieving statistics on parquet files. This might take a while..." << endl;
    }
    CircuitMultiReaderParquet reader(filenames) ;
    CircuitWriterSYN2 writer(output_filename, reader.record_count());

    cout << "Aggregate totals: "
         << reader.record_count() << " records ("
         << reader.block_count() << " blocks)" << endl;

    Converter<CircuitData> converter( reader, writer );

    ProgressMonitor p(reader.block_count());
    converter.setProgressHandler(p.getNewHandler());

    cout << "Converting data" << endl;

    p.task_start();
    converter.exportAll();
    p.task_done();

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

    cerr << endl << "Data copy complete." << endl;
}


void parse_arguments(const int argc, const char* argv[], string& output_filename, vector<string>& input_names)  {
    for(int i=1; i<argc; i++) {
        if(argv[i][0] != '-') {
            input_names.push_back(string(argv[i]));
        }
        else{
            switch( argv[i][1] ) {
                case 'o': {
                    const char* p_name = argv[i]+2;
                    if( p_name[0] == 0 ) {
                        // Option value separated
                        if(++i == argc)  {
                            throw runtime_error("Please provide an argment to -o");
                        }
                        p_name = argv[i];
                    }
                    output_filename = p_name;
                }
            }
        }
    }
}


int main(int argc, const char* argv[]) {
    if(argc < 2) {
        cout << "Usage: \n"
             << "    "  << argv[0] << " [-o output_file] <file1.parquet> [file2.parquet ...]"
             << endl;
        return -1;
    }

    string output_filename("circuit.syn2");
    vector<string> input_names;

    try  {
        parse_arguments(argc, argv, output_filename, input_names);
    }
    catch(const std::exception& e) {
        cout << "Arguments error: " << e.what() << endl;
        return -1;
    }

    /**  Main conversion
     */
    convert_circuit(input_names, output_filename);

    cout << "Creating SYN2 indexes..." << endl;
    {
        syn2::synapses_writer writer(output_filename);
        writer.create_all_index();
    }

    cout << "Finished." << endl;

    return 0;
}
