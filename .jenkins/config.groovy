node {
    library identifier: 'bbp@master', retriever: modernSCM(
        [$class:'GitSCMSource',
         remote: 'ssh://bbpcode.epfl.ch/hpc/jenkins-pipeline'])

    spack("parquet-converters",
          "ssh://bbpcode.epfl.ch/building/ParquetConverters",
          "%gcc",
          test: "salloc -A\$SLURM_PROJECT -n2 ctest -VV")
}
