#include "index.h"

#include <array>
#include <cstdint>
#include <set>
#include <unordered_map>
#include <vector>

#include <mpi.h>

#include <highfive/H5DataSet.hpp>
#include <highfive/H5File.hpp>

namespace indexing {

namespace {

using NodeID = uint64_t;
using RawIndex = std::vector<std::array<uint64_t, 2>>;
using FlatRawIndex = std::vector<std::array<uint64_t, 3>>;

constexpr auto INDEX_ELEMENT_SIZE = sizeof(FlatRawIndex::value_type);

const char* const SOURCE_NODE_ID_DSET = "source_node_id";
const char* const TARGET_NODE_ID_DSET = "target_node_id";

const char* const INDEX_GROUP = "indices";
const char* const SOURCE_INDEX_GROUP = "indices/source_to_target";
const char* const TARGET_INDEX_GROUP = "indices/target_to_source";
const char* const NODE_ID_TO_RANGES_DSET = "node_id_to_ranges";
const char* const RANGE_TO_EDGE_ID_DSET = "range_to_edge_id";

}  // unnamed namespace


namespace {


namespace mpi {

/**
 * \brief Helper to return the current rank.
 */
uint64_t rank() {
    static int rank = -1;
    if (rank < 0) {
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    }
    return rank;
}

/**
 * \brief Helper to return the current MPI size.
 */
uint64_t size() {
    static int size = -1;
    if (size < 0) {
        MPI_Comm_size(MPI_COMM_WORLD, &size);
    }
    return size;
}

/**
 * \brief Helper class to get a RAII-style MPI type
 */
template <typename T>
class DataType {
  public:
    DataType() {
        static_assert(std::is_trivially_copyable<T>::value,
                      "Only trivially copyable types are supported.");
        MPI_Type_contiguous(sizeof(T), MPI_BYTE, &kind_);
        MPI_Type_commit(&kind_);

    }
    ~DataType() {
        MPI_Type_free(&kind_);
    }

    MPI_Datatype& type() {
        return kind_;
    }
  private:
    MPI_Datatype kind_;
};

} // namespace mpi


/**
 * \brief Partition a count and return the offset and range that should be processed.
 *
 * \arg \c num The element count to partition
 * \arg \c rank The rank for which to return the slice to process.  Will be set to the
 * current rank if -1.
 *
 * \returns The offset and count of elements to process
 */
std::pair<uint64_t, uint64_t> partition_count(const uint64_t num, int rank = -1) {
    const auto base_count = num / mpi::size();
    const auto ranks_w_more = num % mpi::size();
    if (rank < 0) {
        rank = mpi::rank();
    }
    return {
        base_count * rank + std::min(static_cast<uint64_t>(rank), ranks_w_more),
        base_count + (rank < ranks_w_more)
    };
}

/**
 * \brief Takes a list of node IDs and groups them into ranges as a flat index.
 */
FlatRawIndex _groupNodeRanges(const std::vector<NodeID>& nodeIDs, uint64_t offset) {
    FlatRawIndex result;

    if (nodeIDs.empty()) {
        return result;
    }

    result.reserve(nodeIDs.size());  // Worst-case scenario, avoid re-allocating a lot

    uint64_t rangeStart = 0;
    NodeID lastNodeID = nodeIDs[rangeStart];
    for (uint64_t i = 1; i < nodeIDs.size(); ++i) {
        if (nodeIDs[i] != lastNodeID) {
            result.push_back({lastNodeID, rangeStart + offset, i + offset});
            rangeStart = i;
            lastNodeID = nodeIDs[rangeStart];
        }
    }

    result.push_back({lastNodeID, rangeStart + offset, nodeIDs.size() + offset});
    result.shrink_to_fit();

    return result;
}

/**
 * \brief Takes a flat index and groups all ranges by node ID in a map.
 */
std::unordered_map<NodeID, RawIndex> _regroupNodeRanges(const FlatRawIndex& nodeRanges) {
    std::unordered_map<NodeID, RawIndex> result;
    for (const auto& [id, start, end]: nodeRanges) {
        auto& ranges = result[id];
        if (!ranges.empty() && ranges.back()[0] <= start && start <= ranges.back()[1]) {
            ranges.back()[1] = std::max(ranges.back()[1], end);
        } else {
            ranges.push_back({start, end});
        }
    }
    return result;
}

/**
 * \brief Returns a list of Node IDs and offset to process for the current rank.
 */
std::pair<std::vector<NodeID>, uint64_t> _readNodeIDs(const HighFive::Group& h5Root, const std::string& name) {
    std::vector<NodeID> result;
    auto dataset = h5Root.getDataSet(name);
    auto [offset, count] = partition_count(dataset.getElementCount());
    if (mpi::rank() == 0) {
      // We have two flat indices in memory concurrently:
      // * the one we read and distribute
      // * the one we gather and write
      // Add a factor of two for potential MPI buffers => 4 indices with count
      // elements.
      //
      // As we process the indices, we should release memory from now prior
      // versions.
      std::cout << "Reading " << count
                << " elements per rank, estimating around "
                << std::ceil((4 * INDEX_ELEMENT_SIZE * count) /
                             (1024.0 * 1024.0))
                << " MB memory usage" << std::endl;
    }
    dataset.select({offset}, {count}).read(result);
    return {result, offset};
}

/**
 * \brief Writes a single dataset.
 */
void _writeIndexDataset(const RawIndex& data, const std::string& name, HighFive::Group& h5Group, uint64_t offset, uint64_t global_count) {
    constexpr auto inner_size = std::tuple_size<RawIndex::value_type>::value;
    auto dset = h5Group.createDataSet<uint64_t>(
        name,
        {global_count, inner_size}
    );
    dset.select({offset, 0}, {data.size(), inner_size}).write(data);
}

/**
 * \brief Writes two datasets: node IDs to ranges, ranges to edge IDs.
 *
 * Will split the work across multiple MPI nodes and gather indices as required.
 */
void _writeIndexGroup(const std::string& source,
                      uint64_t nodeCount,
                      HighFive::Group& h5Root,
                      const std::string& name) {

    auto [nodeIDs, nodeIDOffset] = _readNodeIDs(h5Root, source);
    auto readRanges = _groupNodeRanges(nodeIDs, nodeIDOffset);
    // sort ranges for MPI exchange
    std::sort(std::begin(readRanges), std::end(readRanges));

    if (nodeCount == 0) {
        uint64_t localMaxNodeMaxID = readRanges.back()[0];
        uint64_t globalMaxNodeID;
        MPI_Allreduce(&localMaxNodeMaxID, &globalMaxNodeID, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);
        nodeCount = globalMaxNodeID + 1;
    }

    std::vector<uint64_t> rangeCounts(nodeCount, 0);
    for (const auto& range: readRanges) {
        rangeCounts[range[0]]++;
    }

    std::vector<int> rangesToSend(mpi::size(), 0);
    for (int rank = 0; rank < mpi::size(); ++rank) {
        const auto [offset, count] = partition_count(nodeCount, rank);
        for (int i = offset; i < offset + count; ++i) {
            rangesToSend[rank] += rangeCounts[i];
        }
    }

    // exchange send range
    std::vector<int> rangesToReceive(mpi::size(), 0);
    MPI_Alltoall(
        rangesToSend.data(), 1, MPI_INT,
        rangesToReceive.data(), 1, MPI_INT,
        MPI_COMM_WORLD
    );

    FlatRawIndex writeRanges(std::accumulate(rangesToReceive.begin(), rangesToReceive.end(), uint64_t{0}));
    mpi::DataType<FlatRawIndex::value_type> dt;

    std::vector<int> offsetsToSend(mpi::size() + 1);
    std::partial_sum(rangesToSend.begin(), rangesToSend.end(), offsetsToSend.begin() + 1);

    std::vector<int> offsetsToReceive(mpi::size() + 1);
    std::partial_sum(rangesToReceive.begin(), rangesToReceive.end(), offsetsToReceive.begin() + 1);

    MPI_Alltoallv(
        readRanges.data(), rangesToSend.data(), offsetsToSend.data(), dt.type(),
        writeRanges.data(), rangesToReceive.data(), offsetsToReceive.data(), dt.type(),
        MPI_COMM_WORLD
    );

    {
        FlatRawIndex empty;
        std::swap(readRanges, empty);
    }

    const auto [localNodeOffset, localNodeCount] = partition_count(nodeCount);
    const auto nodeToRanges = _regroupNodeRanges(writeRanges);

    {
        FlatRawIndex empty;
        std::swap(writeRanges, empty);
    }

    const uint64_t rangeCount =
        std::accumulate(nodeToRanges.begin(),
                        nodeToRanges.end(),
                        uint64_t{0},
                        [](uint64_t total, decltype(nodeToRanges)::const_reference item) {
                            return total + item.second.size();
                        });

    std::vector<uint64_t> allRangeCounts(mpi::size());
    MPI_Allgather(
        &rangeCount, 1, MPI_UINT64_T,
        allRangeCounts.data(), 1, MPI_UINT64_T,
        MPI_COMM_WORLD
    );

    const uint64_t localRangeOffset = std::accumulate(allRangeCounts.begin(), allRangeCounts.begin() + mpi::rank(), uint64_t{0});
    const uint64_t globalRangeCount = std::accumulate(allRangeCounts.begin(), allRangeCounts.end(), uint64_t{0});

    RawIndex primaryIndex;
    RawIndex secondaryIndex;

    primaryIndex.reserve(localNodeCount);
    secondaryIndex.reserve(rangeCount);

    uint64_t offset = localRangeOffset;
    for (NodeID nodeID = localNodeOffset; nodeID < localNodeOffset + localNodeCount; ++nodeID) {
        const auto it = nodeToRanges.find(nodeID);
        if (it == nodeToRanges.end()) {
            primaryIndex.push_back({0, 0});
        } else {
            auto& ranges = it->second;
            primaryIndex.push_back({offset, offset + ranges.size()});
            offset += ranges.size();
            std::move(ranges.begin(), ranges.end(), std::back_inserter(secondaryIndex));
        }
    }

    auto indexGroup = h5Root.createGroup(name);
    _writeIndexDataset(primaryIndex, NODE_ID_TO_RANGES_DSET, indexGroup, localNodeOffset, nodeCount);
    _writeIndexDataset(secondaryIndex, RANGE_TO_EDGE_ID_DSET, indexGroup, localRangeOffset, globalRangeCount);
}

}  // unnamed namespace


void write(HighFive::Group& h5Root,
           uint64_t sourceNodeCount,
           uint64_t targetNodeCount) {
    if (h5Root.exist(INDEX_GROUP)) {
        throw std::runtime_error("Index group already exists");
    }

    _writeIndexGroup(SOURCE_NODE_ID_DSET,
                     sourceNodeCount,
                     h5Root,
                     SOURCE_INDEX_GROUP);
    _writeIndexGroup(TARGET_NODE_ID_DSET,
                     targetNodeCount,
                     h5Root,
                     TARGET_INDEX_GROUP);
}


} // namespace index
