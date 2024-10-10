import index_writer_py
from mpi4py import MPI

def init_mpi():
    index_writer_py.init_mpi()

def write_index(filename, source_node_count, target_node_count):
    try:
        index_writer_py.write(filename, source_node_count, target_node_count)
        rank = MPI.COMM_WORLD.Get_rank()
        print(f"Rank {rank} participated in writing index to {filename}")
    except Exception as e:
        rank = MPI.COMM_WORLD.Get_rank()
        print(f"Error writing index on rank {rank}: {e}")
        raise
