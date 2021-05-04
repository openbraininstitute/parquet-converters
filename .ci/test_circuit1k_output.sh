for mode in structural functional; do
    srun parquet2hdf5 \
        $DATADIR/cellular/circuit-1k/touches/$mode/circuit.parquet \
        ${mode}.h5 All
    h5diff --exclude-attribute /edges/All ${mode}.h5 \
        $DATADIR/cellular/circuit-1k/touches/$mode/circuit_from_parquet.h5
done
