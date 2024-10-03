import index_writer_py
import h5py
import numpy as np
from mpi4py import MPI

def create_sample_hdf5_file(filename, source_node_count, target_node_count, comm):
    rank = comm.Get_rank()

    if rank == 0:
        # Generate data
        source = np.random.randint(0, source_node_count, size=1000, dtype=np.uint64)
        target = np.random.randint(0, target_node_count, size=1000, dtype=np.uint64)

        # Create file and datasets
        with h5py.File(filename, 'w') as f:
            f.create_dataset('source_node_id', data=source)
            f.create_dataset('target_node_id', data=target)

    comm.Barrier()  # Ensure file is created before other ranks proceed

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
