import index_writer_py
import h5py
import numpy as np
from mpi4py import MPI

def create_sample_hdf5_file(filename, source_node_count, target_node_count, comm):
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Calculate local chunk size
    chunk_size = 1000 // size
    remainder = 1000 % size
    local_size = chunk_size + (1 if rank < remainder else 0)

    # Generate local data
    local_source = np.random.randint(0, source_node_count, size=local_size, dtype=np.uint64)
    local_target = np.random.randint(0, target_node_count, size=local_size, dtype=np.uint64)

    # Create file with parallel I/O
    with h5py.File(filename, 'w', driver='mpio', comm=comm) as f:
        # Create datasets without parallel access
        if rank == 0:
            f.create_dataset('source_node_id', (1000,), dtype='uint64')
            f.create_dataset('target_node_id', (1000,), dtype='uint64')
        
        comm.Barrier()  # Ensure datasets are created before writing

        # Write local data
        start = rank * chunk_size + min(rank, remainder)
        end = start + local_size
        f['source_node_id'][start:end] = local_source
        f['target_node_id'][start:end] = local_target

def main():
    # Initialize MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Initialize MPI in the C++ module
    index_writer_py.init_mpi()

    filename = "sample_data.h5"
    source_node_count = 100
    target_node_count = 200

    # Create a sample HDF5 file with source and target node IDs
    create_sample_hdf5_file(filename, source_node_count, target_node_count, comm)

    print(f"Rank {rank} of {size} participated in creating file: {filename}")

    # Use the index_writer_py module to write the index
    try:
        index_writer_py.write(filename, source_node_count, target_node_count, comm)
        print(f"Rank {rank} participated in writing index to {filename}")
    except Exception as e:
        print(f"Error writing index on rank {rank}: {e}")

    # Verify the index was written (only on rank 0)
    if rank == 0:
        with h5py.File(filename, 'r') as f:
            if 'indices' in f:
                print("Index group found in the file")
                if 'indices/source_to_target' in f and 'indices/target_to_source' in f:
                    print("Both source_to_target and target_to_source indices are present")
                else:
                    print("One or both indices are missing")
            else:
                print("Index group not found in the file")

if __name__ == "__main__":
    main()
