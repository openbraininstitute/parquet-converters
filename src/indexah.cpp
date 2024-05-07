#include "CLI/CLI.hpp"
#include <highfive/H5File.hpp>

#include <mpi.h>
#include "index/index.h"
#include "version.h"

int
main(int argc, char* argv[]) {
    int mpi_rank = -666;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

    std::string filename;

    CLI::App app{"Add an index to an edge file"};
    app.set_version_flag("-v,--version", neuron_parquet::VERSION);
    app.add_option("filename", filename, "Filename to add indices to")
        ->check(CLI::ExistingFile)
        ->required();

    try {
        app.parse(argc, argv);
    } catch(const CLI::ParseError& e) {
        if (mpi_rank == 0) {
            app.exit(e);
        }
        MPI_Finalize();
        return 1;
    }

    HighFive::File f(filename, HighFive::File::ReadWrite, HighFive::MPIOFileDriver(MPI_COMM_WORLD, MPI_INFO_NULL));
    auto group = f.getGroup("edges");

    for (const auto popname: group.listObjectNames()) {
        try {
            auto pop = group.getGroup(popname);
            indexing::write(pop, 0, 0, true);
        } catch(...) {
        }
    }

    MPI_Finalize();
}
