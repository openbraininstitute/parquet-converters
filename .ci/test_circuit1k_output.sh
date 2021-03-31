for mode in structural functional; do
    srun parquet2hdf5 --format syn2 -o ${mode}.syn2 \
        $DATADIR/cellular/circuit-1k/touches/$mode/circuit.parquet
    h5diff ${mode}.syn2 \
        $DATADIR/cellular/circuit-1k/touches/$mode/circuit_from_parquet.syn2
done
