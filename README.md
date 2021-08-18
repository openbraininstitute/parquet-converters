# Parquet Converters

Converts the binary
[TouchDetector](https://bbpgitlab.epfl.ch/hpc/touchdetector) output to
Parquet to be consumed by
[Spykfunc](https://bbpgitlab.epfl.ch/hpc/circuit-building/spykfunc), and
the final output to [SONATA](https://github.com/BlueBrain/libsonata).

Build with [Spack](https://github.com/BlueBrain/spack):
```
spack install parquet-converters
```
or use the provided module on BlueBrain⑤.

## Usage

To convert TouchDetector output to Parquet (the output directory is the
current directory):
```
srun -n 4 touch2parquet $MY_TD_OUTPUT_DIRECTORY/touchesData.0
```
This will produce 4 Parquet files, adjust the parallelism accordingly to
create more files.

To produce a SONATA file with synapses contained in a population named
`All`:
```
srun -n 100 parquet2hdf5 $MY_FZ_OUTPUT_DIRECTORY/circuit.parquet edges.h5
All
```
Creating the synapse index requires a higher parallelism than the initial
conversion.
