node {
    library identifier: 'bbp@master', retriever: modernSCM(
        [$class:'GitSCMSource',
         remote: 'ssh://bbpcode.epfl.ch/hpc/jenkins-pipeline'])

    spack("parquet-converters",
          "ssh://bbpcode.epfl.ch/building/ParquetConverters",
          "%gcc")
}
