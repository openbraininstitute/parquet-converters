fake_count=10000
seq 1 ${fake_count} > fake.ascii

rm -f fake.h5
h5import \
    fake.ascii -d ${fake_count} -path /nodes/fake/node_type_id \
    fake.ascii -d ${fake_count} -path /nodes/fake/0/bogus_attribute \
    -outfile fake.h5

mode=functional
srun -Aproj16 -pinteractive -n1 \
    parquet2hdf5 \
        --format syn2 \
        --from fake.h5 fake \
        -o ${mode}.syn2 \
        $DATADIR/cellular/circuit-2k/touches/$mode/circuit.parquet

pre_count=($(h5ls -r ${mode}.syn2|awk '/pre\/neuron_id_to_range/ {sub("{", "", $3); sub(",", "", $3); print $3}'))
post_count=($(h5ls -r ${mode}.syn2|awk '/post\/neuron_id_to_range/ {sub("{", "", $3); sub(",", "", $3); print $3}'))

[ ${post_count} -eq 2175 ]
[ ${pre_count} -eq ${fake_count} ]
