import index_writer_py
from mpi4py import MPI
import logging

def get_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

logger = get_logger()

def init_mpi():
    logger.info("Initializing MPI")
    index_writer_py.init_mpi()
    logger.info("MPI initialized")

def write_index(filename, source_node_count, target_node_count):
    rank = MPI.COMM_WORLD.Get_rank()
    size = MPI.COMM_WORLD.Get_size()
    logger.info(f"Rank {rank}/{size}: Starting write_index")
    try:
        logger.info(f"Rank {rank}/{size}: Calling index_writer_py.write")
        index_writer_py.write(filename, source_node_count, target_node_count)
        logger.info(f"Rank {rank}/{size}: Finished index_writer_py.write")
    except Exception as e:
        logger.error(f"Rank {rank}/{size}: Error writing index: {e}")
        raise
    logger.info(f"Rank {rank}/{size}: Completed write_index")
