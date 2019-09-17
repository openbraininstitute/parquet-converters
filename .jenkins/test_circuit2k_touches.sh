srun -Aproj16 -pinteractive -n4 touch2parquet \
    $DATADIR/cellular/circuit-2k/touches/binary/touchesData*

set +x
hash module 2> /dev/null || . /etc/profile.d/modules.sh
module load nix/hpc/spykfunc
set -x

script=$(mktemp)
cat >$script <<EOF
import glob, sys, pyarrow.parquet as pq
sort_cols = ['synapse_id']
base, comp = [pq.ParquetDataset(glob.glob(d)).read().to_pandas() \
                .sort_values(sort_cols).reset_index(drop=True)
              for d in sys.argv[1:]]
print("comparison " + ("successful" if base.equals(comp) else "failed"))
sys.exit(0 if base.equals(comp) else 1)
EOF

python $script \*.parquet $DATADIR/cellular/circuit-2k/touches/parquet/\*.parquet
