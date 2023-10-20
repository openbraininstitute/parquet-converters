/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#include <iostream>

#include "circuit.h"
#include "progress.hpp"


using namespace neuron_parquet;
using namespace neuron_parquet::circuit;
using utils::ProgressMonitor;


void convert_circuit(const std::string & filename)  {
    CircuitReaderParquet reader(filename) ;
    CircuitWriterSYN2 writer(std::string("circuit_syn2"), reader.record_count());

    {
        Converter<CircuitData> converter( reader, writer );
        ProgressMonitor p(reader.block_count());

        converter.setProgressHandler(p);

        converter.exportAll();
    }

    std::cout << "\nComplete." << std::endl;
}

int main(int argc, char* argv[]) {
    if(argc < 2) {
        std::cout << "Please provide a file to convert";
        return -1;
    }
    convert_circuit(std::string(argv[1]));
    return 0;
}
