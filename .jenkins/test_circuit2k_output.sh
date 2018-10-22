for mode in structural functional; do
    srun -Aproj16 -pinteractive -n1 parquet2syn2p -o c.syn2 \
        $DATADIR/cellular/circuit-2k/touches/$mode/circuit.parquet
    h5diff c.syn2 \
        $DATADIR/cellular/circuit-2k/touches/$mode/circuit_from_parquet.syn2
done
