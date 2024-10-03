#include <filesystem>

#include <catch2/catch_test_macros.hpp>
#include <highfive/H5File.hpp>
#include <mpi.h>

#include "index/index.h"

namespace fs = std::filesystem;

const size_t NNODES = 10;
const size_t SOURCE_OFFSET = 90;
const char * GROUP = "data";

class MPIFixture {
  public:
    MPIFixture() {
        int init;
        MPI_Initialized(&init);
        if (!init) {
            MPI_Init(nullptr, nullptr);
        }
        MPI_Comm_size(MPI_COMM_WORLD, &size);
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    }

    ~MPIFixture() {
        int finalized;
        MPI_Finalized(&finalized);
        if (!finalized) {
            MPI_Finalize();
        }
    }

    int size;
    int rank;
};

#define CATCH_CONFIG_RUNNER

void generate_data(const fs::path& base) {
    std::vector<uint64_t> source_ids;
    std::vector<uint64_t> target_ids;
    source_ids.reserve(NNODES * NNODES);
    target_ids.reserve(NNODES * NNODES);

    for (size_t i = 0; i < NNODES; ++i) {
        for (size_t j = 0; j < NNODES; ++j) {
            source_ids.push_back(SOURCE_OFFSET + i);
            target_ids.push_back(j);
        }
    }

    HighFive::File file(base, HighFive::File::Overwrite);
    auto g = file.createGroup(GROUP);
    g.createDataSet("source_node_id", source_ids);
    g.createDataSet("target_node_id", target_ids);
    indexing::write(g, SOURCE_OFFSET + NNODES, NNODES);
}

int main(int argc, char* argv[]) {
    MPIFixture mpi;
    int result = Catch::Session().run(argc, argv);
    return result;
}

TEST_CASE("Indexing") {

    SECTION("Generate data") {
        generate_data("index_test.h5");
    }
    HighFive::File f("index_test.h5");
    auto g = f.getGroup(GROUP);

    std::vector<std::array<uint64_t, 2>> source_ranges;
    std::vector<std::array<uint64_t, 2>> source_edges;
    std::vector<std::array<uint64_t, 2>> target_ranges;
    std::vector<std::array<uint64_t, 2>> target_edges;

    auto gidx = g.getGroup("indices");
    auto sidx = gidx.getGroup("source_to_target");
    auto tidx = gidx.getGroup("target_to_source");
    REQUIRE_NOTHROW(sidx.getDataSet("node_id_to_ranges").read(source_ranges));
    REQUIRE_NOTHROW(sidx.getDataSet("range_to_edge_id").read(source_edges));
    REQUIRE_NOTHROW(tidx.getDataSet("node_id_to_ranges").read(target_ranges));
    REQUIRE_NOTHROW(tidx.getDataSet("range_to_edge_id").read(target_edges));

    SECTION("Read Index") {
        REQUIRE(source_ranges.size() == SOURCE_OFFSET + NNODES);
        REQUIRE(target_ranges.size() == NNODES);

        REQUIRE(source_edges.size() == NNODES);
        REQUIRE(target_edges.size() == NNODES * NNODES);
    }

    SECTION("Check source index") {
        for (size_t i = 0; i < SOURCE_OFFSET; ++i) {
            REQUIRE(source_ranges[i][0] == 0);
            REQUIRE(source_ranges[i][1] == 0);
        }

        for (size_t i = 0; i < NNODES; ++i) {
            REQUIRE(source_ranges[SOURCE_OFFSET + i][0] == i);
            REQUIRE(source_ranges[SOURCE_OFFSET + i][1] == i + 1);

            REQUIRE(source_edges[i][0] == NNODES * i);
            REQUIRE(source_edges[i][1] == NNODES * (i + 1));
        }
    }

    SECTION("Check target index") {
        for (size_t i = 0; i < NNODES; ++i) {
            REQUIRE(target_ranges[i][0] == NNODES * i);
            REQUIRE(target_ranges[i][1] == NNODES * (i + 1));

            // Target node i is connected to every jth source node
            for (size_t j = 0; j < NNODES; ++j) {
                REQUIRE(target_edges[NNODES * i + j][0] == NNODES * j + i);
                REQUIRE(target_edges[NNODES * i + j][1] == NNODES * j + i + 1);
            }
        }
    }
}
